load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

oci_image(
    name = "executed_trades_image",
    base = "@distroless_base",
    entrypoint = ["/executed_trades"],
    tars = [":executed_trades_tar"],
)

oci_push(
    name = "push",
    image = ":executed_trades_image",
    remote_tags = ["latest"],
    repository = ".",
)

oci_load(
    name = "load",
    image = ":executed_trades_image",
    repo_tags = ["irio_executed_trades:latest"],
)

pkg_tar(
    name = "executed_trades_tar",
    srcs = [":executed_trades"],
    package_dir = "/",
)

go_binary(
    name = "executed_trades",
    embed = [":executed_trades_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "executed_trades_lib",
    srcs = [
        "handler.go",
        "main.go"
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/executed-trades",
    visibility = ["//visibility:private"],
    deps = [
        "//common/database",
        "//common/kafka",
        "//proto",
        "@com_github_ibm_sarama//:sarama",
        "@org_golang_google_protobuf//proto",
    ],
)
