require 'rake'

namespace :setup do
  desc 'Install base Python dependencies'
  task :install do
    sh 'pip install -U setuptools'
    sh 'pip install -U pip'
    sh 'pip install -U wheel'
    sh 'pip install -U Cython'
    sh 'pip install -r requirements.txt'
  end
end

namespace :dev do
  desc 'Run linter'
  task :lint do
    sh 'flake8 --show-source --ignore=D10,W503 *.py'
    sh 'isort --check-only --diff --multi-line=5 --trailing-comma .'
  end

  desc 'Clean environment'
  task :clean do
    sh 'find . -type d -name "__pycache__" -exec rm -rf {} + > /dev/null 2>&1'
    sh 'find . -type f -name "*.pyc" -exec rm -rf {} + > /dev/null 2>&1'
  end
end
