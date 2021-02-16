with import <nixpkgs> {};
with pkgs.python37Packages;
stdenv.mkDerivation {
  name = "impurePythonEnv";
  buildInputs = [
    file

    # Docker-related    docker-compose

    # these packages are required for virtualenv and pip to work:
    #
    mypy
    python37Full
    python37Packages.virtualenv

    gcc6
    glibcLocales # for click+python3
    libxml2Python
    mysql57
    ncurses # needed by uWSGI
    openssl
    pcre
    (uwsgi.override { plugins = [ "python3" ]; })
    zlib
  ];
  src = null;
  # TODO: convert to full nix expression so as to not rely on pip
  shellHook = ''
    # set SOURCE_DATE_EPOCH so that we can use python wheels
    SOURCE_DATE_EPOCH=$(date +%s)
    export LANG=en_US.UTF-8
    export LD_LIBRARY_PATH=${mysql57}/lib:${gcc6.cc.lib}/lib:$LD_LIBRARY_PATH
    virtualenv venv
    source venv/bin/activate
    export PATH=$PWD/venv/bin:$PATH
    export PYTHONPATH=$PWD:$PYTHONPATH
    export FLASK_APP=app.py
    export FLASK_DEBUG=1
    pip install pipenv
    pipenv --three install --dev
  '';
}
