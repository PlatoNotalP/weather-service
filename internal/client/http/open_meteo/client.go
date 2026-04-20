package open_meteo

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Response struct {
	Current struct {
		Time        string  `json:"time"`
		Temperature float64 `json:"temperature_2m"`
	}
}

type client struct {
	httpClient *http.Client
}

func NewClient(httpClient *http.Client) *client {
	return &client{
		httpClient: httpClient,
	}
}

func (c *client) GetTemperature(lat, lng float64) (Response, error) {
	resp, err := c.httpClient.Get(
		fmt.Sprintf(
			"https://api.open-meteo.com/v1/forecast?latitude=%f&longitude=%f&current=temperature_2m",
			lat,
			lng,
		),
	)
	if err != nil {
		return Response{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("status code %d", resp.StatusCode)
	}

	var response Response
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return Response{}, err
	}

	return response, nil
}
