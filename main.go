package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
)

// example usage:
// get-s3-tags fh-pi-paguirigan-a SR/ngs/illumina/apaguiri/1151020_SN367_0568_BHFHN2BCXX-Restore/Unaligned/Project_apaguiri

type kvp struct {
	key  string
	tags []s3.Tag
	// names map[string]string
}

func getTags(bucketName string, svc *s3.S3, key string, ch chan<- kvp, guard <-chan struct{}) {
	// fmt.Fprintln(os.Stderr, "hi from getTags!")
	tagInput := &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	tagReq := svc.GetObjectTaggingRequest(tagInput)
	tagResult, err := tagReq.Send()
	if err != nil {
		panic(err)
	}

	// for _, tag := range tagResult.TagSet {
	// 	keynames[*tag.Key] = 1
	// }
	// keymap[*key] = tagResult.TagSet
	// ch <-
	result := kvp{
		key:  key,
		tags: tagResult.TagSet,
	}
	<-guard
	ch <- result
}

func main() {

	if len(os.Args) == 1 || len(os.Args) > 3 {
		fmt.Println("Supply a bucket name and optional prefix.")
		fmt.Println("Redirect output to a file.")
		os.Exit(1)
	}

	maxGoroutines := 10
	guard := make(chan struct{}, maxGoroutines)

	fmt.Fprintln(os.Stderr, "getting bucket listing...")

	bucketName := os.Args[1]

	// maxGoroutines := runtime.NumCPU() - 1
	// guard := make(chan struct{}, maxGoroutines)

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}
	cfg.Region = endpoints.UsWest2RegionID
	svc := s3.New(cfg)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(999999999),
	}

	if len(os.Args) == 3 {
		input.Prefix = aws.String(os.Args[2])
	}

	requests := 0
	keyCount := 0

	tempDir, err := ioutil.TempDir("", "get-s3-tags")
	if err != nil {
		panic(err)
	}

	fmt.Fprintln(os.Stderr, "tempDir is", tempDir) // FIXME remove
	// defer os.RemoveAll(tempDir) // FIXME uncomment

	keyfile, err := os.Create(filepath.Join(tempDir, "keyfile.txt"))
	if err != nil {
		panic(err)
	}

	for {
		req := svc.ListObjectsV2Request(input)
		result, err0 := req.Send()
		if err0 != nil {
			if aerr, ok := err0.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					fmt.Fprintln(os.Stderr, s3.ErrCodeNoSuchBucket, aerr.Error())
				default:
					fmt.Fprintln(os.Stderr, aerr.Error())
					return
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Fprintln(os.Stderr, err0.Error())
				return
			}

		}

		for _, item := range result.Contents {
			keyCount++
			_, err1 := keyfile.WriteString(fmt.Sprintf("%s\n", *item.Key))
			if err != nil {
				panic(err1)
			}
		}

		requests++

		if *result.IsTruncated {
			input.ContinuationToken = result.NextContinuationToken
		} else {
			break
		}

	}

	err = keyfile.Close()
	if err != nil {
		panic(err)
	}

	// fmt.Println("# of requests:", requests)
	// fmt.Println("length of keys:", len(keys))

	// first non-parallel approach (hopefully concurrent though)

	var keymap map[string][]s3.Tag
	keymap = make(map[string][]s3.Tag)

	var keynames map[string]int
	keynames = make(map[string]int)

	ch := make(chan kvp)

	fmt.Fprintln(os.Stderr, "Got listing, getting tags....")

	keysProcessed := 0

	keyfile, err = os.Open(filepath.Join(tempDir, "keyfile.txt"))
	scanner := bufio.NewScanner(keyfile)

	// for _, key := range keys {
	for scanner.Scan() {
		key := scanner.Text()

		keysProcessed++

		if keysProcessed%100 == 0 {
			fmt.Fprintln(os.Stderr, "got ", keysProcessed, "of", keyCount)
		}

		guard <- struct{}{}
		go getTags(bucketName, svc, key, ch, guard)
		// tagInput := &s3.GetObjectTaggingInput{
		// 	Bucket: aws.String(bucketName),
		// 	Key:    key,
		// }
		// tagReq := svc.GetObjectTaggingRequest(tagInput)
		// tagResult, err := tagReq.Send()
		// if err != nil {
		// 	panic(err)
		// }
		//
		// for _, tag := range tagResult.TagSet {
		// 	keynames[*tag.Key] = 1
		// }
		// keymap[*key] = tagResult.TagSet
	}

	fmt.Fprintln(os.Stderr, "number of keys is", keyCount)

	keysProcessed = 0

	jsonFile, err := os.Create(filepath.Join(tempDir, "records.json"))
	if err != nil {
		panic(err)
	}

	_, err = jsonFile.WriteString("[\n")

L:
	for {
		select {
		case item := <-ch:
			keysProcessed++
			var bytesToWrite bytes.Buffer

			var strangs []string
			strangs = append(strangs, fmt.Sprintf(`{ "key": "%s" `, item.key))
			// bytesToWrite.WriteString(fmt.Sprintf(`{ "key": "%s", `, item.key))
			// keymap[item.key] = item.tags
			for _, tag := range item.tags {
				keynames[*tag.Key] = 1
				strangs = append(strangs, fmt.Sprintf(`"%s": "%s"`, *tag.Key, *tag.Value))
			}
			bytesToWrite.WriteString(strings.Join(strangs, ", "))
			bytesToWrite.WriteString("}")
			_, err = jsonFile.WriteString(bytesToWrite.String())
			if err != nil {
				panic(err)
			}
			// do stuf
			if keysProcessed == keyCount {
				break L
			} else {
				_, err = jsonFile.WriteString(",\n")
				if err != nil {
					panic(err)
				}
			}
		}
	}

	_, err = jsonFile.WriteString("\n]\n")
	if err != nil {
		panic(err)
	}
	err = jsonFile.Close()
	if err != nil {
		panic(err)
	}

	// fmt.Println(keymap)
	// fmt.Println("keynames:")
	// fmt.Println(keynames)

	tagColumns := make([]string, 0, len(keynames))
	for keyname := range keynames {
		tagColumns = append(tagColumns, keyname)
	}
	sort.Strings(tagColumns)
	fullTagColumns := append([]string{"key"}, tagColumns...)

	w := csv.NewWriter(os.Stdout)

	if err := w.Write(fullTagColumns); err != nil {
		panic(err)
	}

	for key := range keymap {
		var record []string
		record = append(record, key)
		tags := keymap[key]
		for _, column := range tagColumns {
			found := false
			for _, tag := range tags {
				if *tag.Key == column {
					record = append(record, *tag.Value)
					found = true
					break
				}
			}
			if !found {
				record = append(record, "")
			}
		}
		if err := w.Write(record); err != nil {
			panic(err)
		}
	}

}
