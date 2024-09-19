from os import path

import setuptools
import versioneer

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Get the requirements
with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh]


# All the setups
setuptools.setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name="comicbook",
    description="Source files and dependencies for data pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="SA - Interview Project",
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Programming Language :: Python :: 3",
    ],
    packages=setuptools.find_packages(include=["comicbook*"]),
    install_requires=requirements,
    python_requires=">=3.6, <4",
    entry_points={
        'console_scripts': [
            'job.statistics.Bottom10HeroesByAppearancePerPublisherJob = comicbook.job.statistics.Bottom10HeroesByAppearancePerPublisherJob:main',
            'job.statistics.Bottom10VillainsByAppearancePerPublisherJob = comicbook.job.statistics.Bottom10VillainsByAppearancePerPublisherJob:main',
            'job.statistics.HeroesAndVillainWithTop5SuperpowersJob = comicbook.job.statistics.HeroesAndVillainWithTop5SuperpowersJob:main',
            'job.statistics.Top5SuperpowersJob = comicbook.job.statistics.Top5SuperpowersJob:main',
            'job.statistics.Top10HeroesByAppearancePerPublisherJob = comicbook.job.statistics.Top10HeroesByAppearancePerPublisherJob:main',
            'job.statistics.Top10OverallScoreJob = comicbook.job.statistics.Top10OverallScoreJob:main',
            'job.statistics.Top10SuperpowersPerPublisherJob = comicbook.job.statistics.Top10SuperpowersPerPublisherJob:main',
            'job.statistics.Top10VillainsByAppearancePerPublisherJob = comicbook.job.statistics.Top10VillainsByAppearancePerPublisherJob:main',

        ]
    }
)