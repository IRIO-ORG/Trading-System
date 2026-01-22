load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "database",
    srcs = [
        "client.go",
        "config.go",
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/common/database",
    visibility = ["//visibility:public"],
    deps = [
        "//common",
        "@com_github_lib_pq//:pq",
    ],
)