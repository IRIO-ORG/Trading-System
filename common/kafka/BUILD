load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "kafka",
    srcs = [
        "consumer_runner.go",
        "factory.go",
        "producer.go"
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/common/kafka",
    visibility = ["//visibility:public"],
    deps = [
        "//common",
        "@com_github_ibm_sarama//:sarama",
        "@org_golang_google_protobuf//proto",
    ],
)
