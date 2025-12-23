import subprocess

def run(cmd: str):
    print(f"\n=== {cmd} ===")
    subprocess.check_call(cmd, shell=True)

def main():
    run("python -m src.ingest_yesterday")
    run("python -m src.build_features")
    run("python -m src.score_today")

if __name__ == "__main__":
    main()
