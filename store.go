package main

import (
	"encoding/json"
	"github.com/Mimoja/MFT-Common"
	"github.com/avast/retry-go"
	"github.com/cnf/structhash"
	"io/ioutil"
	"net/http"
	"time"
)

func store(entry MFTCommon.DownloadEntry) error {

	structData := structhash.Dump(entry, 1)

	id := MFTCommon.GenerateID(structData).GetID()

	found, err, oldIEntry := Bundle.DB.Exists("packages", id)
	if err == nil && found == true {
		data, err := oldIEntry.Source.MarshalJSON()
		if err != nil {
			Bundle.Log.WithField("entry", entry).WithError(err).Errorf("Could not get old entry from elastic: %v", err)
		} else {
			//s := string(data)
			//fmt.Println(s)
			err = json.Unmarshal(data, &entry)
			if err != nil {
				Bundle.Log.WithField("entry", entry).WithError(err).Errorf("Could unmarshall old entry from elastic: %v", err)
			} else {
				Bundle.Log.WithField("entry", entry).Infof("Skipping Download: %s", entry.DownloadURL)
				Bundle.MessageQueue.DownloadedQueue.MarshalAndSend(entry)
				return nil
			}

		}
	}

	Bundle.Log.WithField("entry", entry).Info("Starting Download of:", entry.DownloadURL)

	err = retry.Do(
		func() error {
			downloaded, err := DownloadFile(entry)
			if err != nil {
				Bundle.Log.WithField("entry", entry).WithError(err).Warnf("Download failed. Retrieing")
				return err
			}
			entry = downloaded
			return nil
		},
	)

	if err != nil {
		Bundle.Log.WithField("entry", entry).WithError(err).Errorf("Permanently failed to download and hash: %v", err)
		return nil
	}

	entry.DownloadTime = time.Now().Format("2006-01-02T15:04:05Z07:00")
	Bundle.Log.WithField("entry", entry).Debug("Download finished")

	Bundle.DB.StoreElement("packages", nil, entry, &id)
	Bundle.MessageQueue.DownloadedQueue.MarshalAndSend(entry)
	return nil
}

func DownloadFile(entry MFTCommon.DownloadEntry) (MFTCommon.DownloadEntry, error) {

	Bundle.Log.WithField("entry", entry).Debug("Downloading ", entry.DownloadURL, " to ", entry.DownloadPath)

	client := http.Client{
		Timeout: time.Minute * 2,
	}
	resp, err := client.Get(entry.DownloadURL)
	if err != nil {
		Bundle.Log.WithField("entry", entry).WithError(err).Warnf("Could not download entry: %v", err)
		return entry, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		Bundle.Log.WithField("entry", entry).WithError(err).Warnf("Could not read body: %v", err)
		return entry, err
	}

	entry.PackageID = MFTCommon.GenerateID(body)

	err = Bundle.Storage.StoreBytes(body, entry.PackageID.GetID())
	if err != nil {
		Bundle.Log.WithField("entry", entry).WithError(err).Warnf("Could not store entry: %v", err)
		return entry, err
	}

	return entry, nil
}
