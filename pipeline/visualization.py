import matplotlib.pyplot as plt

def plot_ev_increase_by_year(df):
    """
    Plot a bar chart showing the increase in EV cars over model years with two bars per year
    (one for each EV category).

    :param df: DataFrame containing data from the 'vehicles_by_year_and_type' query.
    """
    pivot_df = df.pivot(index="modelYear", columns="electricVehicleType", values="count").fillna(0)
    pivot_df.plot(kind="bar", stacked=False, figsize=(10, 6))

    plt.title("Increase in EV Cars by Model Year")
    plt.xlabel("Model Year")
    plt.ylabel("Number of Vehicles")
    plt.legend(title="EV Type")
    plt.tight_layout()
    plt.savefig("plots/ev_increase_by_year.png")
    plt.close()


def plot_counties_with_most_and_least_evs(df):
    """
    Plot the counties with the highest and least number of EVs for each model year.

    :param df: DataFrame containing data from the 'vehicles_by_county_and_year' query.
    """
    grouped = df.groupby("modelYear")
    most_evs = grouped.apply(lambda x: x.loc[x["count"].idxmax()])
    least_evs = grouped.apply(lambda x: x.loc[x["count"].idxmin()])

    plt.figure(figsize=(12, 6))
    plt.plot(most_evs["modelYear"], most_evs["count"], label="Highest EVs (Counties)", marker="o")
    plt.plot(least_evs["modelYear"], least_evs["count"], label="Lowest EVs (Counties)", marker="o")

    for i, row in most_evs.iterrows():
        plt.text(row["modelYear"], row["count"], f"{row['county']}", fontsize=9, ha="left", rotation=45)
    for i, row in least_evs.iterrows():
        plt.text(row["modelYear"], row["count"], f"{row['county']}", fontsize=9, ha="right", rotation=-45)

    plt.title("Counties with Highest and Least EVs Each Model Year")
    plt.xlabel("Model Year")
    plt.ylabel("Number of EVs")
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/counties_with_most_and_least_evs.png")
    plt.close()


def plot_most_popular_make_by_year(df):
    """
    Plot the most popular EV make for each model year.

    :param df: DataFrame containing data from the 'vehicles_by_make_and_year' query.
    """
    grouped = df.groupby("modelYear").apply(lambda x: x.loc[x["count"].idxmax()])

    plt.figure(figsize=(10, 6))
    plt.bar(grouped["modelYear"], grouped["count"], color="teal", alpha=0.7)

    for i, row in grouped.iterrows():
        plt.text(row["modelYear"], row["count"], f"{row['make']}", fontsize=9, ha="center")

    plt.title("Most Popular EV Make by Model Year")
    plt.xlabel("Model Year")
    plt.ylabel("Number of Vehicles")
    plt.tight_layout()
    plt.savefig("plots/most_popular_make_by_year.png")
    plt.close()
