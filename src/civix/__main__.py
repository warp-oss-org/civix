import argparse

from . import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="civix", description="Civix tooling scaffold.")
    parser.add_argument("--version", action="store_true", help="Print the current version.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(__version__)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
