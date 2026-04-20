package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/PlatoNotalP/weather-service/internal/client/http/geocoding"
	"github.com/PlatoNotalP/weather-service/internal/client/http/open_meteo"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
)

const (
	httpPort = ":8080"
	city     = "vladivostok"
)

type Reading struct {
	Timestamp   time.Time
	Temperature float64
}

type Storage struct {
	data map[string][]Reading
	mtx  sync.RWMutex
}

func initJobs(scheduler gocron.Scheduler, storage *Storage) ([]gocron.Job, error) {
	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	openMeteoClient := open_meteo.NewClient(httpClient)

	j, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				respFromGeocoding, err := geocodingClient.GetCoordinates(city)
				if err != nil {
					log.Println(err)
					return
				}

				respFromOpenMeteo, err := openMeteoClient.GetTemperature(
					respFromGeocoding.Latitude,
					respFromGeocoding.Longitude,
				)
				if err != nil {
					log.Println(err)
					return
				}

				timestamp, err := time.Parse("2006-01-02T15:04", respFromOpenMeteo.Current.Time)
				if err != nil {
					log.Println(err)
					return
				}

				storage.mtx.Lock()
				defer storage.mtx.Unlock()

				storage.data[city] = append(
					storage.data[city],
					Reading{
						Timestamp: timestamp,
						Temperature: respFromOpenMeteo.Current.Temperature,
					},
				)

				log.Println("updated data for city:", city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	storage := &Storage{
		data: make(map[string][]Reading),
	}

	r.Get("/city/{city}", func(w http.ResponseWriter, r *http.Request) {
		cityName := chi.URLParam(r, "city")

		log.Printf("Request for city: %s\n", cityName)

		storage.mtx.RLock()
		defer storage.mtx.RUnlock()

		reading, ok := storage.data[cityName]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
			return
		}

		raw, err := json.Marshal(reading)
		if err != nil {
			log.Println(err)
			return
		}

		_, err = w.Write(raw)
		if err != nil {
			log.Println(err)
		}
	})

	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}

	jobs, err := initJobs(s, storage)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		log.Println("starting server on port", httpPort)
		err = http.ListenAndServe(httpPort, r)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		defer wg.Done()

		log.Printf("starting job: %v\n", jobs[0].ID())
		s.Start()
	}()

	wg.Wait()
}
