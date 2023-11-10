#!/bin/bash

cmake_path="/usr/bin/cmake"

build_path="/root/lihongyu-bustub/build"

config_option="Debug"

lint_target="check-lint"

format_target="check-format"

clang_tidy_target="check-clang-tidy-p2"

submit_target="submit-p2"

jobs_option="50"

# 执行lint目标
$cmake_path --build $build_path --config $config_option --target $lint_target -j $jobs_option --
lint_exit_code=$?

# 执行format目标
$cmake_path --build $build_path --config $config_option --target $format_target -j $jobs_option --
format_exit_code=$?

# 执行clang-tidy目标
$cmake_path --build $build_path --config $config_option --target $clang_tidy_target -j $jobs_option --
clang_tidy_exit_code=$?

# 检查前面步骤是否成功
if [[ $lint_exit_code -eq 0 && $format_exit_code -eq 0 && $clang_tidy_exit_code -eq 0 ]]; then
    # 执行submit目标
    $cmake_path --build $build_path --config $config_option --target $submit_target -j $jobs_option --
else
    echo "格式检查出错，未完成submit。"
fi
