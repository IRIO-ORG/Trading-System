# gazelle:ignore
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

proto_library(
    name = "market_data",
    srcs = ["market_data.proto"],
    deps = [
        "@protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "afe",
    srcs = ["afe.proto"],
    deps = [":market_data"],
)

go_proto_library(
    name = "proto",
    compilers = [
        "@rules_go//proto:go_grpc",
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/proto",
    protos = [
        ":afe",
        ":market_data",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
