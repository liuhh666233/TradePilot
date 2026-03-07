{ lib, buildPythonPackage, fetchFromGitHub, hatchling, numpy, pandas, requests
}:

buildPythonPackage rec {
  pname = "akracer";
  version = "unstable-2025-09-10";
  pyproject = true;

  src = fetchFromGitHub {
    owner = "akfamily";
    repo = "akracer";
    rev = "2a368edb10d6d5662868af8ca1d819dde059d0df";
    hash = "sha256-W256umLf91SyIwTfInSdVgVhxJAb+EmZ5PnGYwTuOTo=";
  };

  build-system = [ hatchling ];

  dependencies = [ numpy pandas requests ];

  doCheck = false;
  dontUsePythonImportsCheck = true;

  meta = {
    description =
      "AKRacer is a third-party extension library for py_mini_racer";
    homepage = "https://github.com/akfamily/akracer";
    license = lib.licenses.mit;
    maintainers = with lib.maintainers; [ ];
  };
}
