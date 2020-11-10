import setuptools

setuptools.setup(
    name="pylim",
    version="1.1.14",
    author="aeorxc",
    author_email="author@example.com",
    description="wrapper around morningstar commodity (LIM)",
    url="https://github.com/aeorxc/pylim",
    project_urls={
        'Source': 'https://github.com/aeorxc/pylim',
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['pandas', 'lxml', 'requests', 'tables', 'commodutil'],
    python_requires='>=3.6',
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)

