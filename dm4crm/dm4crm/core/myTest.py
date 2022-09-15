import json

from models.schema.schema import Schema
from models.workspace import Workspace


ws: Workspace = Workspace.get_workspace()
ws.engine_type = 'pandas'
ws.new_engine()
# -----------------------------------
node_id0 = ws.create_node("CSVReader", file_path=r"C:\Users\mhset\OneDrive\Desktop\telco_churn.csv", read_header=True)
node_id1 = ws.create_node("CaseWhen", new_column_name="CHURN", columns=["LEAVE"],
                          rules=[("LEAVE == 'STAY'", 0), (True, 1)])
node_id2 = ws.create_node("DropColumn", target_column=["COLLEGE", "REPORTED_SATISFACTION", "REPORTED_USAGE_LEVEL",
                                                       "CONSIDERING_CHANGE_OF_PLAN", "LEAVE"])
node_id3 = ws.create_node("TrainTestSplit", train_size=0.8)
node_id4 = ws.create_node("SeparateTargetColumn", target_column=["CHURN"])
node_id5 = ws.create_node("SeparateTargetColumn", target_column=["CHURN"])
node_id9 = ws.create_node("Duplicate")
node_id6 = ws.create_node("DecisionTreeClassifier", max_depth=5)
node_id7 = ws.create_node("Predict", target_name="CHURN")
# node_id8 = ws.create_node("Concat")
# node_id10 = ws.create_node("Concat")
node_id11 = ws.create_node("Score")
# node_id2 = ws.create_node("SelectColumn", selected_columns=['play', 'windy'], alias={'play': 'not_play'})
# node_id3 = ws.create_node("ChangeColumnType", new_type={'windy': 'str'})
# node_id4 = ws.create_node("Sort", selected_columns=['windy'], ascending=False)
# node_id5 = ws.create_node("Shuffle")
# node_id6 = ws.create_node("ChangeColumnType", new_type={'windy': 'str'})
# node_id7 = ws.create_node("SplitColumn", target_column="windy", out_columns=["dec", "frac"], sep=".", delete_old=True)
# node_id8 = ws.create_node("ConstantColumn", column_name='new_col', value='5.5', col_type="float64")
# -----------------------------------
ws.connect_nodes(node_id0, node_id1)
ws.connect_nodes(node_id1, node_id2)
ws.connect_nodes(node_id2, node_id3)
ws.connect_nodes(node_id3, node_id4)
ws.connect_nodes(node_id3, node_id5, 1, 0)
ws.connect_nodes(node_id4, node_id6)
ws.connect_nodes(node_id4, node_id6, 1, 1)
ws.connect_nodes(node_id6, node_id7)
ws.connect_nodes(node_id5, node_id9, 0, 0)
ws.connect_nodes(node_id9, node_id7, 0, 1)
# ws.connect_nodes(node_id7, node_id8, 0, 1)
# ws.connect_nodes(node_id9, node_id8, 1, 0)
# ws.connect_nodes(node_id5, node_id10, 1, 1)
# ws.connect_nodes(node_id8, node_id10, 0, 0)
# ws.connect_nodes(node_id8, node_id10, 0, 0)
ws.connect_nodes(node_id7, node_id11)
ws.connect_nodes(node_id5, node_id11, 1, 1)


# -----------------------------------
ws.compile(node_id11)
x = ws.show()
x = json.dumps(x, indent=4)
print(x)
