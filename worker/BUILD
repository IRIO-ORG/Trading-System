load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "worker_lib",
    srcs = ["main.go"],
    importpath = "github.com/IRIO-ORG/Trading-System/worker",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "worker",
    embed = [":worker_lib"],
    visibility = ["//visibility:public"],
)
