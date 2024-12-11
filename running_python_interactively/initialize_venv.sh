# -- Ensure mamba activate can be called
eval "$(conda shell.bash hook)"
CONDA_BASE=$(conda info --base)
source $CONDA_BASE/etc/profile.d/mamba.sh 

# -- Create venv
mamba create --name v_jupyter python=3.12 -y
mamba activate v_jupyter
mamba install --file requirements.txt -y