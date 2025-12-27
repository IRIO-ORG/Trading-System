load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

oci_image(
    name = "afe_image",
    base = "@distroless_base",
    entrypoint = ["/afe"],
    tars = [":afe_tar"],
)

# e.g. bazel run //afe:push -- --repository ghcr.io/<ORG>/image
# Pushes the image to the registry, target of the registry (`repository` parameter) is set in github actions.
oci_push(
    name = "push",
    image = ":afe_image",
    remote_tags = ["latest"],
    repository = ".",  # Overwritten by GH actions
)

# Loads the image to local docker. Use only for development.
oci_load(
    name = "load",
    image = ":afe_image",
    repo_tags = ["irio_afe:latest"],
)

pkg_tar(
    name = "afe_tar",
    srcs = [":afe"],
    package_dir = "/",
)

go_binary(
    name = "afe",
    embed = [":afe_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "afe_lib",
    srcs = [
        "main.go",
        "server.go",
    ],
    importpath = "github.com/IRIO-ORG/Trading-System/afe",
    visibility = ["//visibility:private"],
    deps = [
        "//common",
        "//proto",
        "@com_github_ibm_sarama//:sarama",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
