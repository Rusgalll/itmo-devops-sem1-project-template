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

func main() {
	godotenv.Load()
	dsn := "postgres://" + os.Getenv("DB_USER_NAME") + ":" + os.Getenv("DB_PASSWORD") +
		"@" + os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT") +
		"/" + os.Getenv("DB_NAME") + "?sslmode=" + os.Getenv("DB_SSL_MODE")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	router := mux.NewRouter()

	router.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "Failed to retrieve file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, file); err != nil {
			http.Error(w, "Failed to read file", http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			http.Error(w, "Invalid zip file", http.StatusBadRequest)
			return
		}

		var records []PriceData
		for _, f := range zipReader.File {
			if len(f.Name) < 4 || f.Name[len(f.Name)-4:] != ".csv" {
				continue
			}
			rc, err := f.Open()
			if err != nil {
				continue
			}
			csvReader := csv.NewReader(rc)
			csvReader.Read()
			for {
				rec, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if len(rec) < 5 {
					continue
				}
				price, _ := strconv.ParseFloat(rec[3], 64)
				date, _ := time.Parse("2006-01-02", rec[4])
				records = append(records, PriceData{
					ID:        rec[0],
					CreatedAt: date,
					Name:      rec[1],
					Category:  rec[2],
					Price:     price,
				})
			}
			rc.Close()
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
	}).Methods("POST")

	http.ListenAndServe(":"+os.Getenv("APP_PORT"), router)
}
