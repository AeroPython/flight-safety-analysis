![aeropython_logo](./images/aeropython_logo.png)


# Analysis of flight accidents with python

This project is a continuation of the work done for the following talks:

* [PyData Warsaw 2017 - Analysing flight safety data with python](https://github.com/AeroPython/pydata_warsaw2017_flight_safety):
  - [Video](https://www.youtube.com/watch?v=8mly9yYdK0M)
  - [Slides](http://nbviewer.jupyter.org/format/slides/github/AeroPython/pydata_warsaw2017_flight_safety/blob/master/slides.ipynb#/)
* [PyData Madrid 2016 - Remove before flight: Analysing flight safety data with Python](https://github.com/AeroPython/remove-before-flight)


## Motivation

A continuous improvement in flight safety is a common goal for all public agencies and private companies. Many analysis are published every year but, would it be possible to reproduce those same results? This project uses a real open database from the American National Transportation Safety Board (NTSB) in order to answer a bunch of questions about aircraft safety.

Python has become a very useful tool for data science. This project is aimed at anyone who is interested in data analysis, statistics or machine learning in Python having the added incentive of dealing with real data from the aviation authorities. 

This is the right place either if you are interested in aircraft safety (even if you know little about Python) or you are looking for an interesting project to apply your data science knowledge (even if you know little about aviation).


## Objectives

Aircraft Safety:

 - Agencies involved and reports they publish
 - Hot topics and common questions
 - Find evidences based on data for common claims about aircraft safety (ie. is it the safest mean of transport?)
 - Answer new questions that could be asked based on the available data
 - Ask new questions that could be answered if other data was available

Data Analysis with Python:
  
 - Gain experience on data science work flow: gathering, understanding, exploring, cleaning and mining the data
 - Improve on the use of pandas for the previous tasks:
   - Available functions and methods that make some analysis more straightforward
   - Efficient use of DataFrames
 - Generate proper visualizations of the results to highlight the most important features
 - Advance on interactive techniques and tools (Notebooks, widgets, bokeh) to encourage the pursuit of new insights about the data


# Contributing

## I have a suggestion or an aircraft safety question I think that could be answered as part of the project

Great! Please go to the issues section, choose a good title for the issue and elaborate your question or sugestion as much as you can:

  * Which is the concrete question you would like to answer?
  * What data do would you use for the analysis? Is it in the data sources we are using? If it is: which file/table/column? are they well populated? If it is not: do you have any idea on where to find it?
  * How would you make the data reduction: filtering conditions, aggregation techniques (counts, mean, variance...), machine learning algorithms...?
  * How would you visualize the results: tables, plots...? What kind of plot?
  * Is there any other place where this question has been answered before? Can we use it to compare our results?


## Requirements

It is recommended to use Anaconda/miniconda (https://www.anaconda.com/download) to install the dependencies. After conda is installed, the environment.yml can be used:

```
$ conda env create -f environment.yml
```


## Getting the data

As the database is too big to be uploaded to the repository it is stored separately [here](https://www.dropbox.com/s/n9inalri0dvff1j/avall.db?dl=1). It can be placed in the `data` folder manually or downloaded using `flight_safety/get_data.py` python script.

The data can also be obtained directly from NTSB website (https://app.ntsb.gov/avdata/) in `mdb` format. In order to convert it to a SQLite database the script `export_access2csv.sh` in `utils` folder can be used:

```
$ bash utils/export_access2csv.sh data/raw/avall.mdb data
```

## Making a contribution

> If it is your first contribution to a project on GitHub, this might be difficult. Don't worry and let us help you! If you get stuck into any stage of the following procedure don't hesitate to ask for help. Our telegram group [Aeropython](https://t.me/AeroPython) can serve this purpose.


Try to avoid `push` to the [main repo](https://github.com/AeroPython/flight-safety-analysis) branches ( :no_entry_sign:`master` or `0.1.x`), and `push` to your own fork. Then, open a `pull request` that will be thoroughly reviewed by the admins and added.

First, **fork** the [master repository](https://github.com/AeroPython/flight-safety-analysis) to your Github (green button, upper right).

Then, in your terminal :computer::
```
$ git clone https://github.com/User_Name/flight-safety-analysis.git
$ cd flight-safety-analysis/
$ git remote add upstream https://github.com/AeroPython/flight-safety-analysis.git
```

To add changes:

```
$ git checkout -b my-changes  # Create a new working branch (local) to play around and test
# Then changes are made
$ git push origin my-changes  # This "pushes" the changes to your own fork (aka Github)
```

Then create a pull request. The code will be reviewed :mag:. After it has been merged by the reviewer, your repo's master must be updated. In your terminal:

```
$ git checkout master  # Switch to local master branch
$ git fetch upstream  # Changes made to the original repo are collected
$ git merge --ff-only upstream/master  # Updating! (from original repo's master branch)
$ git branch --delete my-changes  # Working branch (local) is no longer needed.
$ git push origin --delete mis-cambios  # Remote branch isn't needed neither, http://stackoverflow.com/a/2003515/554319
$ git push origin master  # Updates fork (Github's)
```

If for any reason the merge of your pull request is delayed, it might happen that other pull request have indeed gone through the scrutiny and the remote master has changed. It is convenient to update our working branch, using `git merge`†:

```
$ git checkout master  # Switch to master branch (local)
$ git fetch upstream  # Changes made to the original repo are collected
$ git merge --ff-only upstream/master  # Updates master branch (local), changes made to the original repo are implemented
$ git push origin master  # Updates master branch (remote, Github's fork)
$ git checkout my-changes  # Switch to the working branch (local)
$ git merge master  # Updates the working branch (local)
```

† For more information about this: https://www.atlassian.com/git/articles/git-team-workflows-merge-or-rebase/
with drawings and stuff: https://www.atlassian.com/git/tutorials/merging-vs-rebasing/workflow-walkthrough
