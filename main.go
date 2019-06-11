package main

import (
	"MimojaFirmwareToolkit/pkg/Common"
	"encoding/json"
)

var Bundle MFTCommon.AppBundle

func worker(id int, file <-chan MFTCommon.DownloadEntry) {

	for true {
		load := <-file
		Bundle.Log.WithField("entry", load).Infof("Handeling %s in Worker %d\n", load.DownloadURL, id)
		store(load)
	}
}

const NumberOfWorker = 3

func main() {
	Bundle = MFTCommon.Init("Downloader")

	downloads := make(chan MFTCommon.DownloadEntry, NumberOfWorker)
	for w := 1; w <= NumberOfWorker; w++ {
		go worker(w, downloads)
	}

	Bundle.MessageQueue.URLQueue.RegisterCallback("Downloader", func(payload string) error {
		var file MFTCommon.DownloadEntry
		err := json.Unmarshal([]byte(payload), &file)
		if err != nil {
			Bundle.Log.WithField("payload", payload).Error("Could not unmarshall json")
		}

		Bundle.Log.WithField("entry", file).Debug("Queuing entry")
		downloads <- file

		return nil
	})
	Bundle.Log.Info("Starting up!")
	select {}
}
