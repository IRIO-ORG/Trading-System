#!/bin/bash
# Adapted from https://github.com/golang/go/blob/88da9ccb98ffaf84bb06b98c9a24af5d0a7025d2/misc/git/pre-commit
GOROOT=$(bazel run @rules_go//go -- env GOROOT | tr -d '\r')
GOFMT_BIN="$GOROOT/bin/gofmt"

gofiles=$(find . -path 'bazel-*' -prune -o -name '*.go' -print)
echo "Go files:" $gofiles
[ -z "$gofiles" ] && exit 0

unformatted=$($GOFMT_BIN -l $gofiles)
[ -z "$unformatted" ] && exit 0

# Some files are not gofmt'd. Print message and fail.
echo >&2 "Go files must be formatted with gofmt. Please run:"
for fn in $unformatted; do
	echo >&2 "  gofmt -w $PWD/$fn"
done

exit 1
