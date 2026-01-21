load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

oci_image(
    name = "worker_image",
    base = "@distroless_base",
    entrypoint = ["/worker"],
    tars = [":worker_tar"],
)

# e.g. bazel run //worker:push -- --repository ghcr.io/<ORG>/image
# Pushes the image to the registry, target of the registry (`repository` parameter) is set in github actions.
oci_push(
    name = "push",
    image = ":worker_image",
    remote_tags = ["latest"],
    repository = ".",  # Overwritten by GH actions
)

# Loads the image to local docker. Use only for development.
oci_load(
    name = "load",
    image = ":worker_image",
    repo_tags = ["irio_worker:latest"],
)

pkg_tar(
    name = "worker_tar",
    srcs = [":worker"],
    package_dir = "/",
)

go_binary(
    name = "worker",
    embed = [":worker_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "worker_lib",
    srcs = [
        "dump.go",
        "engine.go",
        "main.go",
        "orderbook.go",
        "snapshot.go",
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/worker",
    visibility = ["//visibility:private"],
    deps = [
        "//common",
        "//common/kafka",
        "//proto",
        "@com_github_ibm_sarama//:sarama",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
