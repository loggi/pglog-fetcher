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

// Dowloads
func main() {

	// flags. love flags.
	region := flag.String("region", "sa-east-1", "AWS region code.") // default is 'são paulo' region
	retries := flag.Int("retries", 10, "Number of retries")
	chunkSize := flag.Int64("chunk-size", int64(100), "Size of each portion fetched")
	napDuration := flag.Duration("nap-duration", 500 * time.Millisecond, "Nap time between portion fetch")
	logLevel := flag.String("log-level", "info", "Log level of this app, not RDS PG, as defined by logrus package")
	instanceId := flag.String("instance-id", "theInstanceId", "RDS instance id") // this one is mandatory, shouldn't be really a flag. oh well...
	retrievedFileDir := flag.String("retrieved-file-dir", "/tmp", "Dir where to put downloaded files")
	flag.Parse()

	if len(os.Args) <= 1 {
		flag.PrintDefaults()
		return
	}

	level, error := log.ParseLevel(*logLevel)
	check(error, "Level not understood", log.Fields{"logLevel": logLevel})
	log.SetLevel(level)

	log.Infoln("All set. Starting.")

	// Steps
	//
	// Looping while true (has to run as a service after daemonizing it)
	//     Loop until discover file to download (check recover situations)
	//     Loop while the file has content not downloaded AND there is no new file to download
	//         download, append and save file content
	svc := rds.New(aws.NewConfig().WithRegion(*region).WithMaxRetries(*retries))

	logFilename := logFileDiscover(svc, instanceId)
	portion, _ := downloadLogFilePortion(svc, instanceId, logFilename, "0", *chunkSize)

	filename := *retrievedFileDir + "/" + *logFilename
	splitedPath := strings.Split(filename, "/")
	dir := strings.Join(splitedPath[:len(splitedPath) -1], "/")
	os.MkdirAll(dir, 0700)
	log.WithFields(log.Fields{
		"filename": filename,
		"splitedPath": splitedPath,
		"dir": dir,
	}).Info()

	f, err := os.Create(filename)
	check(err, "Error creating", log.Fields{"filename":  filename})
	defer f.Close()

	_, err = f.WriteString(*portion.LogFileData)
	check(err, "Couldn't write to file", log.Fields{"filename" :filename})

	// check current and expected number of lines
	for *portion.AdditionalDataPending {
		time.Sleep(*napDuration)
		portion, _ = downloadLogFilePortion(svc, instanceId, logFilename, *portion.Marker, *chunkSize)
		_, err = f.WriteString(*portion.LogFileData)
		check(err, "Couldn't write to output file T_T", log.Fields{})
		f.Sync()
	}

	log.Infoln("Done. Should not get here, really.")
}

func logFileDiscover(svc *rds.RDS, instanceId *string) *string {
	// TODO XXX seiti - remove line below. this one is a good log, as it is very small (241 lines)
	test := aws.String("error/postgresql.log.2015-09-24-10")
	log.Debugln(test)

	return test
	// TODO seiti - uncomment
	//	resp, err := listLogFiles(svc, instanceId)
	// check(err, "")
	//	last := resp.DescribeDBLogFiles[len(resp.DescribeDBLogFiles) -1]
	//	log.Debugln(last)
	//	return last
}


func listLogFiles(svc *rds.RDS, instanceId string) (*rds.DescribeDBLogFilesOutput, error) {
	params := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(instanceId),
	}
	resp, err := svc.DescribeDBLogFiles(params)
	check(err, "Couldn't get the logs list", log.Fields{ "instanceId": instanceId})
	log.Debugln(resp.DescribeDBLogFiles)
	return resp, err
}

func downloadLogFilePortion(svc *rds.RDS, instanceId *string, logFilename *string, marker string, nlines int64) (*rds.DownloadDBLogFilePortionOutput, error) {
	params := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: instanceId,
		LogFileName:          logFilename,
		Marker:               aws.String(marker),
		NumberOfLines:        aws.Int64(nlines),
	}

	log.Debugln(fmt.Sprintf("req marker %v: %v", params.Marker, *params.Marker))
	resp, err := svc.DownloadDBLogFilePortion(params)
	check(err, "Couldn't get a portion of file", log.Fields{ "logFilename":  *params.LogFileName})
	log.Debugln(fmt.Sprintf("resp marker %v: %v", resp.Marker, *resp.Marker))

	log.Debugln(*resp.LogFileData)
	log.Debugln(fmt.Sprintf("is additional data pending: '%v'", *resp.AdditionalDataPending))
	return resp, err
}

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
