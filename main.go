package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type PriceData struct {
	ID        string
	CreatedAt time.Time
	Name      string
	Category  string
	Price     float64
}

type PostResponse struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
}

func initializeDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func parseCSVFileFromZip(zf *zip.File) ([]PriceData, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	if _, err := csvReader.Read(); err != nil {
		return nil, err
	}

	var items []PriceData
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("CSV read error: %v", err)
			continue
		}
		if len(record) < 5 {
			continue
		}
		price, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			log.Printf("Price parse error: %v", err)
			continue
		}
		createdAt, err := time.Parse("2006-01-02", record[4])
		if err != nil {
			log.Printf("Date parse error: %v", err)
			continue
		}
		item := PriceData{
			ID:        record[0],
			Name:      record[1],
			Category:  record[2],
			Price:     price,
			CreatedAt: createdAt,
		}
		items = append(items, item)
	}
	return items, nil
}

func handlePostPrices(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "Failed to retrieve file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		zipBuffer := new(bytes.Buffer)
		if _, err := io.Copy(zipBuffer, file); err != nil {
			http.Error(w, "File read error", http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.NewReader(bytes.NewReader(zipBuffer.Bytes()), int64(zipBuffer.Len()))
		if err != nil {
			http.Error(w, "Invalid zip file", http.StatusBadRequest)
			return
		}

		var records []PriceData
		for _, f := range zipReader.File {
			if filepath.Ext(f.Name) != ".csv" {
				continue
			}
			csvRecords, err := parseCSVFileFromZip(f)
			if err != nil {
				log.Printf("Error parsing %s: %v", f.Name, err)
				continue
			}
			records = append(records, csvRecords...)
		}

		processed := 0
		for _, rec := range records {
			_, err := db.Exec("INSERT INTO prices (id, created_at, name, category, price) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
				rec.ID, rec.CreatedAt, rec.Name, rec.Category, rec.Price)
			if err == nil {
				processed++
			}
		}

		var catCount int
		var totalPrice float64
		row := db.QueryRow("SELECT COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices")
		row.Scan(&catCount, &totalPrice)

		resp := PostResponse{
			TotalItems:      processed,
			TotalCategories: catCount,
			TotalPrice:      math.Round(totalPrice*100) / 100,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func handleGetPrices(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
		if err != nil {
			http.Error(w, "Data retrieval error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var records []PriceData
		for rows.Next() {
			var id int
			var createdAt time.Time
			var name, category string
			var price float64
			if err := rows.Scan(&id, &createdAt, &name, &category, &price); err != nil {
				log.Printf("Row scan error: %v", err)
				continue
			}
			records = append(records, PriceData{
				ID:        strconv.Itoa(id),
				CreatedAt: createdAt,
				Name:      name,
				Category:  category,
				Price:     price,
			})
		}

		buf := new(bytes.Buffer)
		csvWriter := csv.NewWriter(buf)
		csvWriter.Write([]string{"id", "name", "category", "price", "create_date"})
		for _, p := range records {
			csvWriter.Write([]string{
				p.ID,
				p.Name,
				p.Category,
				strconv.FormatFloat(p.Price, 'f', 2, 64),
				p.CreatedAt.Format("2006-01-02"),
			})
		}
		csvWriter.Flush()

		zipBuf := new(bytes.Buffer)
		zipWriter := zip.NewWriter(zipBuf)
		f, err := zipWriter.Create("data.csv")
		if err != nil {
			http.Error(w, "Zip file creation error", http.StatusInternalServerError)
			return
		}
		f.Write(buf.Bytes())
		zipWriter.Close()

		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
		w.Write(zipBuf.Bytes())
	}
}

func main() {
	godotenv.Load()
	dsn := "postgres://" + os.Getenv("DB_USER_NAME") + ":" + os.Getenv("DB_PASSWORD") +
		"@" + os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT") +
		"/" + os.Getenv("DB_NAME") + "?sslmode=" + os.Getenv("DB_SSL_MODE")
	db, err := initializeDB(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	router := mux.NewRouter()
	router.HandleFunc("/api/v0/prices", handlePostPrices(db)).Methods("POST")
	router.HandleFunc("/api/v0/prices", handleGetPrices(db)).Methods("GET")
	http.ListenAndServe(":"+os.Getenv("APP_PORT"), router)
}
