load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "common",
    srcs = [
        "env.go",
        "interceptors.go",
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/common",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)
