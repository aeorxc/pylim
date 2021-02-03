import setuptools

setuptools.setup(
    name="pylim",
    version="1.3.0",
    author="aeorxc",
    description="Wrapper around morningstar commodity (LIM)",
    url="https://github.com/aeorxc/pylim",
    project_urls={
        "Source": "https://github.com/aeorxc/pylim",
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["pandas", "lxml", "requests", "commodutil"],
    python_requires=">=3.8",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
