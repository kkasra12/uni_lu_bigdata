import glob
from tokenize import PlainToken
from matplotlib import pyplot as plt
import pandas as pd


executions_times = {
    f.split("_")[-1].split(".")[0]: pd.read_csv(f)["Execution time"]
    for f in glob.glob("report*.csv")
}
df = pd.DataFrame(executions_times)
df.plot()
plt.savefig("report.png")
plt.show()
