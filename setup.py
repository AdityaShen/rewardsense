from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="rewardsense",
    version="0.1.0",
    description="A Cost-Aware, Explainable Credit Card Recommendation System",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Aditya Shenoy, Akhilesh Kasturi, Arjun Vinay Avadhani, Rahul Suresh, Vidya Kalyandurg",
    author_email="team@rewardsense.com",
    url="https://github.com/avadharj/rewardsense",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.4",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "pytest-asyncio>=0.23.3",
            "black>=24.1.1",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
            "isort>=5.13.2",
            "ruff>=0.2.1",
        ],
        "docs": [
            "sphinx>=7.2.6",
            "sphinx-rtd-theme>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "rewardsense-api=app.main:main",
            "rewardsense-scraper=data_pipeline.scrapers.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="credit-cards, recommendations, mlops, machine-learning, fintech",
)
