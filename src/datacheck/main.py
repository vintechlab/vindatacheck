from datacheck.core.spark import get_spark_session
from datacheck.config import parse_args
from datacheck.processors.comparator import compare


def main():
    spark = get_spark_session("DataCheck")
    config = parse_args()

    compare(spark, config.source, config.target)


if __name__ == "__main__":
    main()
