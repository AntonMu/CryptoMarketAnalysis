# Crypto Market Analysis
 
### Installation

#### Setting up Environment [Linux or Mac]

Clone this repo with:
```
git clone https://github.com/AntonMu/CryptoMarketAnalysis
cd CryptoMarketAnalysis/
```
Create Virtual Environment (requires [venv](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/) which is included in the standard library of Python 3.3 or newer):
```
python3 -m venv env
source env/bin/activate
```
Make sure that, from now on, you **run all commands from within your virtual environment**.

#### Setting up Environment [Windows]
Use the [Github Desktop GUI](https://desktop.github.com/) to clone this repo to your local machine. Navigate to the `TrainYourOwnYOLO` project folder and open a power shell window by pressing **Shift + Right Click** and selecting `Open PowerShell window here` in the drop-down menu.

Create Virtual Environment (requires [venv](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/) which is included in the standard library of Python 3.3 or newer):

```
py -m venv env
.\env\Scripts\activate
```

Make sure that, from now on, you **run all commands from within your virtual environment**.

#### Install Required Packages [Windows, Mac or Linux]
To install the packages run:

```
pip3 install -r requirements.txt
```


### Run Parser on AWS
To run the download script on AWS, we use tmux. Check out a cheat sheet [here](https://gist.github.com/henrik/1967800).