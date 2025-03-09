import matplotlib.pyplot as plt
import seaborn as sns

# Example dataset
data = sns.load_dataset("example_dataset")

# Create a line plot with faceting by branch city
g = sns.FacetGrid(data, col="branch_city", col_wrap=4)
g.map(sns.lineplot, "date", "sales_figures", "branch")

plt.show()
