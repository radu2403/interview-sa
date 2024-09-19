# SA - Data Code Challenge

Here is your SA data code challenge!

## The Task

We want to do some research on different types of comic characters. One of our
team members has collected these files in the data directory:

| File Name                    | Description                                                     |
|------------------------------|-----------------------------------------------------------------|
| `comic_characters_info.csv`  | General character stats                                         |
| `dc-data.csv`                | Names, total appearances and more for DC characters             |
| `marvel-data.csv`            | Names, total appearances and more for marvel characters         |
| `hero-abilities.csv`         | Names, superpowers, and more for many Superheroes and Villains  |

The `comic_characters_info.csv` file contains the following information (note only
relevant data has been commented):

| Column     | Description                              |
|------------|------------------------------------------|
| ID         | Sequential number                        |
| Name       | The name of the character                |
| Alignment  | If the character is Good, Bad or Neutral |
| Gender     |                                          |
| EyeColor   |                                          |
| Race       |                                          |
| HairColor  |                                          |
| Publisher  | Marvel Comics, DC Comics etc             |
| SkinColor  |                                          |
| Height     |                                          |
| Weight     |                                          |

We would like you to use the files listed above to show the following:

* Top 10 villains by appearance per publisher 'DC', 'Marvel' and 'other'
* Top 10 heroes by appearance per publisher 'DC', 'Marvel' and 'other'
* Bottom 10 villains by appearance per publisher 'DC', 'Marvel' and 'other'
* Bottom 10 heroes by appearance per publisher 'DC', 'Marvel' and 'other'
* Top 10 most common superpowers by creator 'DC', 'Marvel' and 'other'
* Of the top 10 villains and heroes, re-rank them based on their overall score
* What are the 5 most common superpowers?
* Which hero and villain have the 5 most common superpowers?

## Developer assumptions

1) This is a competitions project and the config files will not be created. Config classes will normally read from a config file.
2) The output normally should be sunk to a delta table, that would follow the same architecture principals as the reading from the source. We here simplify this and just return the output in for of a Df
3) We are omitting things like cluster config, deployment and pipelines
4) Using the package/wheel in prod should be done via the entrypoints in the setup.py file

## Architecture overview

The main components are:
* Jobs
* DAOs
* Configs

They are forced to be immutable via the "dataclasses", and have "private" fields in order to ensure the initial structure of the object that is set in the \__post_init\__ 

### Job
* The job is the main entry point in the code.
* Each job is isolated from the others, as they are unique.
* Each job has one scope/target.
* It contains the necessary code to get the information that it needs and process the data.
* It has a immutable aspect to it
* Everything is configured in the __post_init__ that acts like the initialization method. It follows an architecture similar to the "Factory method"

### DAOs
* The DAOs, or Data Access Objects, are objects that help with the access of a particular data source
* Their job is only for access (read/write/delete/update) and not to add logic, **except** if you want to trigger a subset of that storage

### Configs
* Are objects that interact with the different configurations that a job receives and gathers them in an object
* If a class needs parameters, it will have a dedicated config class
* You can look at them like the Entity classes or Case Classes (in Scala), just a bucket of information
* Each Class will have only the properties that are needed by the class that uses it


