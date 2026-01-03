load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "kafka",
    srcs = ["factory.go"],
    importpath = "github.com/IRIO-ORG/Trading-System/common/kafka",
    visibility = ["//visibility:public"],
    deps = [
        "//common",
        "@com_github_ibm_sarama//:sarama",
        ],
)
