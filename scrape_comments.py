import os
import re
import json
from tabulate import tabulate
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

# Initialize the YouTube Data API client
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)


def get_video_comments(video_id, max_results=100):
    try:
        comments = []

        # Get the initial set of comments
        results = (
            youtube.commentThreads()
            .list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                maxResults=max_results,
            )
            .execute()
        )

        # Keep fetching comments until there are no more pages
        while results:
            for item in results["items"]:
                comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                comments.append(comment)

            # Get the next page of comments (if available)
            nextPageToken = results.get("nextPageToken")
            if nextPageToken:
                results = (
                    youtube.commentThreads()
                    .list(
                        part="snippet",
                        videoId=video_id,
                        textFormat="plainText",
                        maxResults=max_results,
                        pageToken=nextPageToken,
                    )
                    .execute()
                )
            else:
                break

        return comments

    except HttpError as e:
        # print(f"An error occurred: {e}")
        # catch quota error
        if "quota" in str(e):
            error_details = json.dumps(e.error_details)
            error_details = json.loads(error_details)
            if error_details[0]["reason"] == "quotaExceeded":
                raise Exception("quota error")
        return []


def extract_video_id_from_filename(filename):
    # Use regular expression to extract the YouTube video ID
    match = re.search(r"_([A-Za-z0-9_-]+)\.csv", filename)

    if match:
        video_id = match.group(1)
        return video_id
    else:
        return None


def read_comments_data_dir_exclude_list():
    """
    Reads the comments data directory and returns a list of all the comments
    """
    exclude_video_ids = []
    for filename in os.listdir(
        "/Users/aditya.karnam/Projects/muscrape_yt_comments/comments_data"
    ):
        if filename.endswith(".csv"):
            video_id = extract_video_id_from_filename(filename)
            if video_id:
                exclude_video_ids.append(video_id)
    return exclude_video_ids


def print_summary(skipped_video_ids, scraped_video_ids, errored_video_ids, video_ids):
    data = [
            ("Skipped Videos", skipped_video_ids),
            ("Scraped Videos", scraped_video_ids),
            ("Errored Videos", errored_video_ids),
            ("Total Videos", skipped_video_ids + scraped_video_ids + errored_video_ids),
            ("Total Videos Scraped", len(read_comments_data_dir_exclude_list())),
            ("Total Videos Remaining", len(video_ids) - len(read_comments_data_dir_exclude_list()))
        ]
    table = tabulate(data, headers=["Category", "Count"])
    print("---------------------------------")
    print("Session Summary")
    print(table)
    print("---------------------------------")


if __name__ == "__main__":
    # read the music dataset csv
    music_dataset_df = pd.read_csv("indian_music_dataset.csv")

    # get the video ids
    video_ids = music_dataset_df["video_id"].tolist()

    #reverse the video ids
    video_ids.reverse()

    skipped_video_ids = 0
    scraped_video_ids = 0
    errored_video_ids = 0

    # get the comments for each video id
    for video_id in video_ids:
        if video_id in read_comments_data_dir_exclude_list():
            # print("skipping video id: {}".format(video_id))
            skipped_video_ids += 1
            continue
        try:
            print("processing video id: {}".format(video_id))
            comments = get_video_comments(video_id)
            # dump comments as dataframe
            df = pd.DataFrame(comments, columns=["comment"])
            df.to_csv(
                "comments_data/comments_{}.csv".format(
                    video_id
                ),
                index=False,
            )
            scraped_video_ids += 1
        except Exception as e:
            print("error for video id: {}".format(video_id))
            errored_video_ids += 1
            # catch quota error
            if "quota" in str(e):
                print("quota error")
                break
            continue
        print_summary(skipped_video_ids, scraped_video_ids, errored_video_ids, video_ids)
    print_summary(skipped_video_ids, scraped_video_ids, errored_video_ids, video_ids)
