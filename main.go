package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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
		fmt.Println("More documentation available at:")
		fmt.Println("https://github.com/FredHutch/get-s3-tags")
		os.Exit(1)
	}

	maxGoroutines := 30 // started with 10 (it works)
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

	defer os.RemoveAll(tempDir)

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

	fmt.Fprintln(os.Stderr, "done getting keys, total ==", keyCount)

	// fmt.Println("# of requests:", requests)
	// fmt.Println("length of keys:", len(keys))

	// first non-parallel approach (hopefully concurrent though)

	// var keymap map[string][]s3.Tag
	// keymap = make(map[string][]s3.Tag)

	var keynames map[string]int
	keynames = make(map[string]int)

	//
	ch := make(chan kvp)

	keysProcessed := 0

	jsonFile, err := os.Create(filepath.Join(tempDir, "records.json"))
	if err != nil {
		panic(err)
	}

	_, err = jsonFile.WriteString("[\n")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// fmt.Fprintln(os.Stderr, "in select for loop and keysProcessed is", keysProcessed)

			select {
			case item := <-ch:
				// fmt.Fprintln(os.Stderr, "keys processed:", keysProcessed)

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
				// fmt.Fprintln(os.Stderr, "writing to json file!")
				_, err = jsonFile.WriteString(bytesToWrite.String())
				if err != nil {
					panic(err)
				}
				// do stuf
				if keysProcessed == keyCount {

					_, err = jsonFile.WriteString("\n]\n")
					if err != nil {
						panic(err)
					}
					err = jsonFile.Close()
					if err != nil {
						panic(err)
					}

					return
				}
				_, err = jsonFile.WriteString(",\n")
				if err != nil {
					panic(err)
				}
			}
		}

	}()

	fmt.Fprintln(os.Stderr, "Got listing, getting tags....")

	keyfile, err = os.Open(filepath.Join(tempDir, "keyfile.txt"))
	scanner := bufio.NewScanner(keyfile)

	keysProcessed2 := 0

	// for _, key := range keys {
	for scanner.Scan() {
		key := scanner.Text()

		keysProcessed2++

		if keysProcessed2%1000 == 0 {
			fmt.Fprintln(os.Stderr, "got ", keysProcessed2, "of", keyCount)
		}

		guard <- struct{}{}
		go getTags(bucketName, svc, key, ch, guard)
	}
	// fmt.Fprintln(os.Stderr, "before wait")
	wg.Wait()
	// fmt.Fprintln(os.Stderr, "after wait")

	fmt.Fprintln(os.Stderr, "number of keys is", keyCount)

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
	defer w.Flush()

	if err2 := w.Write(fullTagColumns); err2 != nil {
		panic(err2)
	}

	jsonFile, err = os.Open(filepath.Join(tempDir, "records.json"))
	defer jsonFile.Close()
	dec := json.NewDecoder(jsonFile)
	_, err = dec.Token() // opening [
	if err != nil {
		panic(err)
	}

	count := 1

	for dec.More() {
		// fmt.Fprintln(os.Stderr, "writing record", count, "to csv")
		count++
		// for key := range keymap {
		var m map[string]string
		err = dec.Decode(&m)
		if err != nil {
			panic(err)
		}
		// fmt.Fprintln(os.Stderr, m)
		var record []string
		record = append(record, m["key"])
		// tags := keymap[m["key"]]
		// fmt.Fprintln(os.Stderr, tagColumns)
		for _, column := range tagColumns {
			found := false
			for tag, value := range m {
				// fmt.Fprintln(os.Stderr, tag)
				if tag == column {
					record = append(record, value)
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

	// read closing bracket
	_, err3 := dec.Token()
	if err3 != nil {
		panic(err3)
	}

}
