load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "afe",
    embed = [":afe_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "afe_lib",
    srcs = ["main.go"],
    importpath = "github.com/IRIO-ORG/Trading-System/afe",
    visibility = ["//visibility:private"],
)
