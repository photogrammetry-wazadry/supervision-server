import os
import numpy as np
from glob import glob
import shutil


def get_levenshtein_distance(first_word: str, second_word: str) -> int:
    n, m = len(second_word), len(first_word)
    dp = np.zeros((m + 1, n + 1))

    dp[0][0] = 0
    for j in range(1, n + 1):
        dp[0][j] = dp[0][j - 1] + 1

    for i in range(1, m + 1):
        dp[i][0] = dp[i - 1][0] + 1
        for j in range(1, n + 1):
            if first_word[i - 1] != second_word[j - 1]:
                dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1,
                              dp[i - 1][j - 1] + 1)
            else:
                dp[i][j] = dp[i - 1][j - 1]

    return int(dp[m][n])


for dir_name in os.listdir("output/"):
    zip_name = ''.join(dir_name.split("_")[1:]) + ".zip"

    done_path = os.path.join("done/", zip_name)
    target_zip = os.path.join("output/", dir_name, "input.zip")
    
    if os.path.exists(target_zip):
        continue

    if os.path.exists(done_path):
        shutil.move(done_path, target_zip)
    else:
        minn = 1e9 + 7
        best_file = ""

        for done_file in os.listdir("done/"):
            dist = get_levenshtein_distance(zip_name, done_file)

            if dist < minn:
                minn = dist
                best_file = done_file

        if best_file != "":
            if minn <= 10:
                print(minn, zip_name, best_file)
                shutil.move(os.path.join("done/", best_file), target_zip)

