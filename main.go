package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"strings"
)

type Param struct {
	svc *rds.RDS
	instanceId *string
	fetchNap *time.Duration
	chunkSize *int64
	portionNap *time.Duration
	retrievedFileDir *string
	runAsService *bool
}

var p Param

// Downloads last log found at AWS RDS, as it is generated. Does not, currently,
// get the older logs.
func main() {

	// flags. love flags.
	region := flag.String("region", "sa-east-1", "AWS region code.") // default is 'são paulo' region
	retries := flag.Int("retries", 10, "Number of retries")
	chunkSize := flag.Int64("chunk-size", int64(10000), "Size of each portion fetched")
	fetchNap := flag.Duration("fetch-nap", 1 * time.Minute, "Nap time between fetches. Each fetch usually creates a single file")
	portionNap := flag.Duration("portion-nap", 10 * time.Second, "Nap time between each portion fetch")
	logLevel := flag.String("log-level", "info", "Log level of this app, not RDS PG, as defined by logrus package")
	instanceId := flag.String("instance-id", "theInstanceId", "RDS instance id") // this one is mandatory, shouldn't be really a flag. oh well...
	retrievedFileDir := flag.String("retrieved-file-dir", "/tmp", "Dir where to put downloaded files")
	runAsService := flag.Bool("service", false, "Should run indefinitely.")
	flag.Parse()

	if len(os.Args) <= 1 {
		flag.PrintDefaults()
		return
	}

	level, error := log.ParseLevel(*logLevel)
	check(error, "Level not understood", log.Fields{"logLevel": logLevel})
	log.SetLevel(level)

	svc := rds.New(aws.NewConfig().WithRegion(*region).WithMaxRetries(*retries))
	p = Param {
		svc: svc,
		instanceId: instanceId,
		fetchNap: fetchNap,
		chunkSize: chunkSize,
		portionNap: portionNap,
		retrievedFileDir: retrievedFileDir,
		runAsService: runAsService,
	}

	log.WithFields(log.Fields{
		"instanceId": *p.instanceId,
		"fetchNap": *p.fetchNap,
		"chunkSize": *p.chunkSize,
		"portionNap": *p.portionNap,
		"runAsService": *p.runAsService,
		"retrievedFileDir": *p.retrievedFileDir,
		"logLevel": level,
	}).Info("All set. Starting.")

	// Steps
	// Looping while true (has to run as a service after daemonizing it)
	//     Discover file to download (TODO check recover situations)
	//     Loop while the file has content not downloaded AND there is no new file to download
	//         download, append and save file content
	firstLoop := true
	var marker = "0"
	var currMarker = "0"
	var pglog = aws.String("")
	for *p.runAsService || firstLoop {
		currPglog := logFileDiscover(p)

		// transition time!
		log.WithFields(log.Fields{
			"pglog": *pglog,
			"currPgLog": *currPglog,
		}).Debug("Transition check")
		if *pglog != "" && *currPglog != *pglog {
			// retrieve remainder of last pglog
			log.Debug("Transition time! A new log appears!")
			fetchData(p, pglog, &marker)
			pglog = currPglog
			marker = "0"
		}

		currMarker = fetchData(p, currPglog, &currMarker)
		firstLoop = false
		log.WithFields(log.Fields{
			"pglog": *pglog,
			"currPgLog": *currPglog,
		}).Debug("Fetching nap ZZZzzzz...")
		time.Sleep(*p.fetchNap)
	}
	log.Infoln("Done. Should not get here, really.")
}

// Fetches the log data from RDS.
func fetchData(p Param, pglog *string, marker *string) string {
	currMarker := marker
	portion := downloadLogFilePortion(p, pglog, *currMarker)
	filename := *p.retrievedFileDir + "/" + *pglog + "_" + fmt.Sprintf("%v", time.Now().Unix())
	f := createFile(filename)
	defer f.Close()
	_, err := f.WriteString(*portion.LogFileData)
	f.Sync()
	check(err, "Couldn't write to file", log.Fields{"filename" :filename})
	for *portion.AdditionalDataPending {
		log.Debugln("Portion nap ZZZzzzz...")
		time.Sleep(*p.portionNap)
		currMarker = portion.Marker
		portion = downloadLogFilePortion(p, pglog, *currMarker)
		_, err = f.WriteString(*portion.LogFileData)
		check(err, "Couldn't write to output file T_T", log.Fields{"filename" :filename})
		f.Sync()
	}
	return *portion.Marker
}

// Creates a file where to put the fetched data.
func createFile(filename string) *os.File {
	splittedPath := strings.Split(filename, "/")
	dir := strings.Join(splittedPath[:len(splittedPath) - 1], "/")
	os.MkdirAll(dir, 0700)
	log.WithFields(log.Fields{
		"filename": filename,
		"dir": dir,
	}).Info("Creating/opening file")
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	check(err, "Error creating/openning", log.Fields{"filename":  filename})
	return f
}

// Get the log files from AWS RDS and returns the last one found.
func logFileDiscover(p Param) *string {
	resp := listLogFiles(p)
	last := resp.DescribeDBLogFiles[len(resp.DescribeDBLogFiles) -1]
	log.WithFields(log.Fields{"pglog_discovered": last}).Info()
	return last.LogFileName
}

// List the log files.
func listLogFiles(p Param) *rds.DescribeDBLogFilesOutput {
	params := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: p.instanceId,
	}
	resp, err := p.svc.DescribeDBLogFiles(params)
	check(err, "Couldn't get the logs list", log.Fields{ "instanceId": p.instanceId})
	return resp
}

// Download a portion of the given log file. Uses `marker` to control.
func downloadLogFilePortion(p Param, pglog *string, marker string) (*rds.DownloadDBLogFilePortionOutput) {
	params := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: p.instanceId,
		LogFileName:          pglog,
		Marker:               aws.String(marker),
		NumberOfLines:        p.chunkSize,
	}
	log.WithFields(log.Fields{
		"marker_addr": params.Marker,
		"marker_value": *params.Marker,
		"pglog": *pglog,
	}).Debug("Request")
	resp, err := p.svc.DownloadDBLogFilePortion(params)
	check(err, "Couldn't get a portion of file", log.Fields{ "logFilename":  *params.LogFileName})
	log.WithFields(log.Fields{
		"marker_addr": resp.Marker,
		"marker_value": *resp.Marker,
		"additional_data_pending": *resp.AdditionalDataPending,
		"fetched_data_len": len(*resp.LogFileData),
	}).Debug("Response")
	return resp
}

// Checks if a error ocurred, panicking it did.
func check(err error, panicMsg string, panicFields log.Fields) {
	if err == nil {
		return
	}
	log.WithError(err)
	if awsErr, ok := err.(awserr.Error); ok {
		log.WithFields(log.Fields{
			"awserr_code": awsErr.Code(),
			"awserr_msg": awsErr.Message(),
		}).Error()
		if origErr := awsErr.OrigErr(); origErr != nil {
			log.WithError(origErr)
		}
	}
	log.WithFields(panicFields).Panic(panicMsg)
	panic(panicMsg)
}
