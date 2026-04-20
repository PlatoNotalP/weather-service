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

const httpPort = ":8080"

func initJobs(scheduler gocron.Scheduler) ([]gocron.Job, error) {
	j, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				log.Println("Hello")
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

	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	openMeteoClient := open_meteo.NewClient(httpClient)

	r.Get("/city/{city}", func(w http.ResponseWriter, r *http.Request) {
		city := chi.URLParam(r, "city")

		log.Printf("Request for city: %s\n", city)

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

		raw, err := json.Marshal(respFromOpenMeteo)
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

	jobs, err := initJobs(s)
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
