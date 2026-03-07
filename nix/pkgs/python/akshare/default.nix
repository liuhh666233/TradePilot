{ lib, buildPythonPackage, fetchFromGitHub, setuptools, wheel, aiohttp
, beautifulsoup4, decorator, html5lib, jsonpath, lxml, akracer, nest-asyncio
, openpyxl, pandas, requests, tabulate, tqdm, urllib3, xlrd }:

buildPythonPackage rec {
  pname = "akshare";
  version = "1.17.83";
  pyproject = true;

  src = fetchFromGitHub {
    owner = "akfamily";
    repo = "akshare";
    rev = "release-v${version}";
    hash = "sha256-HZ88ebYQ2KbHiM6eoDwoVAQr0+Iw5BOWYHBZNRx11YE=";
  };

  build-system = [ setuptools wheel ];

  dependencies = [
    aiohttp
    beautifulsoup4
    decorator
    html5lib
    jsonpath
    lxml
    akracer
    nest-asyncio
    openpyxl
    pandas
    requests
    tabulate
    tqdm
    urllib3
    xlrd
  ];

  pythonImportsCheck = [ "akshare" ];

  pythonRemoveDeps = [ "aiohttp" "py-mini-racer" ];

  postPatch = ''
    substituteInPlace setup.py \
      --replace-fail "aiohttp>=3.11.13" "aiohttp>=3.10.0"
  '';

  meta = {
    description =
      "AKShare is an elegant and simple financial data interface library for Python, built for human beings! 开源财经数据接口库";
    homepage = "https://github.com/akfamily/akshare";
    license = lib.licenses.mit;
    maintainers = with lib.maintainers; [ ];
  };
}
