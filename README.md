# get-s3-tags

A tool to retrieve the tags for every key in an S3 bucket
(or every key that starts with a prefix, if a prefix is supplied)
and return the information in a CSV file.

## usage

When run without arguments, the program will print a brief help message.

Example usage is as follows:

```
get-s3-tags bucket-name <prefix> > out.csv
```

The first argument is the name of an S3 bucket. You must have read access to the bucket
and must have AWS credentials in your `~/.aws` directory.

The second (optional) argument is a prefix. If supplied, the program will
only return keys that start with the prefix. Example of usage with prefix:

```
get-s3-tags bucket-name foo/bar/baz > out.csv
```

This will return all the keys in `bucket-name` that start with `foo/bar/baz`.

The CSV output is printed to standard out, so you must redirect output to a file.

## structure of csv file

The first column will always be the key; the rest of the columns consist of whatever tag names
were found in the bucket (or prefix of the bucket). Even if a tag name only occurs on one
object, it will get its own column. Column names are sorted alphabetically
(with the exception of `key` which is always first).

## performance

The program will process a bucket with half a million keys in about 15 minutes.
If you give it a prefix to restrict it to ~100 keys it will finish in about a second.


## to run automatically

You can schedule the program to run automatically at a given interval using
`cron`.

Enter the `crontab -e` command to add an entry to your crontab. If you have not
run this before, it will ask you which editor you want to use to edit the crontab.

Add an entry like the following. (Note that `/app/bin` is the path where the
program is installed on the `rhino` and `gizmo` systems).

```
15 1 * * * /app/bin/get-s3-tags my-bucket > $HOME/out.csv 2>> $HOME/get-s3-tags.log
```

What this means: "at 1:15 AM every day, run `get-s3-tags` on the
`my-bucket` bucket, creating a CSV file called `out.csv` in my home
directory, and appending logs to `get-s3-tags.log` in my home directory."

If the CSV file is not produced as expected, you can look in the log file
for clues to see what went wrong.

For more information on crontab syntax, see [this link](http://www.adminschoice.com/crontab-quick-reference).

## problems?

File an [issue](https://github.com/FredHutch/get-s3-tags/issues/new) or email
`scicomp`.
