from setuptools import setup, find_packages

with open('requirements.txt') as fp:
    install_requires = fp.read()

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='faustprometheus',
    version="0.0.3",
    author='Wood Mackenzie',
    author_email='stefan.kecskes@woodmac.com',
    description="WoodMac's Faust Prometheus Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/woodmac/faust-prometheus",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Topic :: System :: Monitoring"
    ],
    install_requires=install_requires,
    zip_safe=True,
    python_requires='>=3.6'
)
