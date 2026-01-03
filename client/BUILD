load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "client",
    embed = [":client_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "client_lib",
    srcs = ["main.go"],
    importpath = "github.com/IRIO-ORG/Trading-System/client",
    visibility = ["//visibility:private"],
    deps = [
        "//proto",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
