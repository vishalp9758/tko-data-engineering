import sys

def main(temp_f) -> float:
#    decimal.getcontext().prec = 35
    return (float(temp_f) - 32) * (5/9)

# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
