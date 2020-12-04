# TuraLabs Data Engineering Bootcamp

Learn to deploy end-to-end Serverless Data Engineering Pipelines on GCP via the most comprehensive and FREE online course.

## Description

This repo contains the code for the [TuraLabs Data Engineering Bootcamp(DEB)](https://www.turalabs.com/docs). Code scaffolds and starting data mentioned in the DEB lessons are contained in this repo.

## QuickLinks

- [Course Pre-requisites](#course-pre-requisites)
- [Setting up your Dev Environment](#setting-up-your-dev-environment)
- [Need Help?](#need-help)
- [Suggestions](#suggestions)
- [Patch Notes](#patch-notes)


## Course Pre-requisites

This course immediately starts covering mid to high level topics. Therefore, we strongly recommend that learners have some experience with **Python** and **SQL**. For a more in depth explanation of what Pre-requisites are expected and a list of resrouces to bring you up to speed, please visit our blog post on [Helpful Resources to Prep for this Course](http://turalabs.com/blog/pre-reqs).

## Setting Up Your Dev Environment

The DEB course uses `Python` and `Google Cloud Platform(GCP)` tools. Please follow the instructions in our [Getting Started Guide](https://www.turalabs.com/docs/getting-started/user-setup) to make sure dev environment is properly set up and compatible with our course. If you have any issues getting your dev environment up, pleaes visit our [Discord Channel](https://discord.gg/xW8JnTm) to talk to us.

## Need Help?
We're here to help! If you have any questions, please connect with us on our [Discord Channel](https://discord.gg/xW8JnTm) and one of us would be happy to help you!

## Suggestions

If you have any suggestions for the course or website, please feel free to open a [GitHub Issue](https://github.com/turalabs/deb/issues) within this repo. We also welcome suggestions in the [suggestion channel on our Discord Server](https://discord.gg/bHDX6tb).

## Patch Notes

### 20201204 Patch Notes:

_Chapter 1_
- Fixed typos in chapter 1 overview (thank you Senad)

_Chapter 2_
- Added GCS source file download instructions to chapter 2 episode 2 (thank you Jason)
- Fixed API registration link in chapter 2 episode 4
- Updated Portman API documentation links in chapter 2 episode 4
- Enhanced chapter 2 episode 5 webapp

_Chapter 3_
- Chapter 3 Intro to Spark episode release

_Blog_
- Added Spark Explained blog post


### 20201117 New Blog Post + Small Fixes

_General_

- Fixed broken link this GitHub [issue](https://github.com/turalabs/deb/issues/7)
- Fixed typos

_Website_
- Add Curriculum Overview page (/deb-info)
- Add Registration page (/register)
- Add About Us page (/about)

_Blog_
- Add “What is Data Engineering? How is it different from Data Science?” Blog post

### 20201021 Ch1 Update + Small Fixes

_General_

- Switching Slack to Discord 
- Fixed broken links to GCP Console and external documents

_Ch1_

New Ch1Ep5 lesson for advanced pandas use to replace the aircraft dataset to the latest FAA records


### 20200930 Ch2 Update + Small Fixes

**Website**

_General_

- fix broken links

_Ch1_

- add introductory episode on Pandas and Jupyter Notebook for beginners https://dev-dot-turalabs-site.uc.r.appspot.com/docs/ch1/c1e5
- add note to clarify continuous use of the same GCS bucket throughout chapter

_Ch 2_

- add note in Episode 5 pointing towards Slack channel if you have any questions running a React App 

_Blog_

- add Pandas and Jupyter Notebook episode as a standalone blog post

**DEB Repo**

_Ch 2_

- updated API and webapp for end of chapter to fix CORS issue
- updated flights API request based on new query syntax
- updated webapp READMEs to refer to Getting Started docs to acquire service account key

### 20200923 Small Fixes

_General_

- fix typos
- fix broken links
- WSL User Setup
- point to windows WSL2 initial setup
- WSL vs WSL2: https://docs.microsoft.com/en-us/windows/wsl/compare-versions
- Mention install ubuntu 20.04 from MS Store
- fix using python3.7
- add instructions to install python3.7 from deadsnake repos
- adding pip to PATH: home/{username}/.local/bin to my PATH in Ubuntu in order to get access to pip
- fix installing and setting up pip3 and virtualenv
- fix creating a new virtualenv
- remove --no-site-packages from virtualenv instructions
- add instructions for deactivate

_Chapter 1_

-change paths in code examples to reflect location of data in provided repo
