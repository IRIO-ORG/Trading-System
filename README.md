# Trading System - Distributed computing infrastructure engineering
This project is part of Distributed computing infrastructure engineering course at University of Warsaw during the 2025/2026 academic year.

The course is realised in collaboration with Google and one of the employees was a mentor.

Authors: Tomasz Głąb, Hubert Krupniewski. Piotr Wieczorek

## Bazel cheatsheet

Fixup BUILD files, for example auto add dependencies:
```sh
bazel run //:gazelle
```

Depend on a new tool:
```sh
bazel run @rules_go//go -- get -tool your_tool_repo.com/x/useful/tool
```

To add a new external dependency from [Bazel Central Registry](https://registry.bazel.build/), add the dependency to `MODULE.bazel`:
```
bazel_dep(name = "my_library", version = "1.0.0")
```
