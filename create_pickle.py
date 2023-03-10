import argparse
import importlib
import os
import pickle


def create_pickled_file(pickle_file_path, mapr_app):
    module = importlib.import_module("{}.{}".format("mrapps", mapr_app))
    with open(os.path.join(pickle_file_path, mapr_app + ".pb"), "wb") as fd:
        pickle.dump({"mapper": module.mapper, "reducer": module.reducer}, fd)


def main():
    parser = argparse.ArgumentParser(
        prog="Pickle Creator",
        description="Create the pickle file",
    )
    parser.add_argument(
        "--mapr-app",
        required=True,
        help="Name of the python file in directory mrapps without .py extension",
    )

    parser.add_argument(
        "--pickle-file-path", help="Path of the pickled map reduce job file"
    )

    args = parser.parse_args()

    create_pickled_file(args.pickle_file_path, args.mapr_app)


if __name__ == "__main__":
    main()
