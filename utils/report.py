def init():
    global Report
    Report = []


def html_report():
    table = '''
        <!DOCTYPE html>
<html>
<head>
<style>
table {
  font-family: arial, sans-serif;
  border-collapse: collapse;
  width: 100%;
}

td, th {
  border: 1px solid #dddddd;
  text-align: left;
  padding: 8px;
}

tr:nth-child(even) {
  background-color: #dddddd;
}
</style>
</head>
<body>

<h2>data validation report</h2>

<table>
  <tr>
    <th>Table</th>
    <th>Source</th>
    <th>Destination</th>
    <th>Diff count</th>
  </tr>
  <replace_this_tr>
</table>

</body>
</html>
    '''

    tr_data = '''
     <tr>
    <td>table</td>
    <td>source_count</td>
    <td>destination_count</td>
    <td>diff_count</td>
  </tr>
    '''
    final_tr = ""

    for table_data in Report:
        for key, value in table_data.items():
            final_tr = final_tr + tr_data.replace("table", key).replace("source_count",
                                                                        str(value["src_total_count"])).replace(
                "destination_count", str(value["dst_total_count"])).replace("diff_count",
                                                                                           str(value["diff_count"]))

    final_table = table.replace("<replace_this_tr>", final_tr)

    html_file = open("E:\manohar\data_validation\\report\html_report.html", "w")
    html_file.write(final_table)
    html_file.close()
