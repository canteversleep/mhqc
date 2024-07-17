from typing import Dict, List, Any
import os
import gzip
import pickle
import csv
import requests
import json
import ijson
import copy


### Networking things

def stream_spans(endpoint, traceid):
    """
    Takes in the Jaeger endpoint and a trace ID, and returns a JSON object with the spans for each ID.
    
    Args:
        endpoint (str): The Jaeger API endpoint.
        traceid (str): A string representing a trace ID.
    
    Returns:
        dict: A JSON object containing the spans for the given trace ID.
    """
    try:
        # Construct the full URL
        url = f"{endpoint}/api/traces/{traceid}"
        
        # Send a GET request to the endpoint
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Return the JSON response
            return response.json()
        else:
            # Return an error message if the request was not successful
            return {"error": f"Failed to retrieve spans. Status code: {response.status_code}"}
    except requests.RequestException as e:
        # Handle any exceptions that occur during the request
        return {"error": str(e)}


## Procesing traces

def stream_traces(f, out, compression=True, debug=False):
    """
    Stream traces from Jaeger API and write to CSV file. If debug is true, also return 
    list of traces. It implements a caching mechanism. For a given directory, it keeps
    track of which traceIDs have already been collected from previous services and
    skips them.
    """
    def openf(f, compression):
        if compression:
            return gzip.open(f, 'wt', compresslevel=9, newline='')
        return open(f, 'w', newline='')
    # read pickle file with existing trace IDs
    traceIDs = set()
    try:
        with open(os.path.dirname(out) + '/.traceIDs.pickle', 'rb') as cache:
            traceIDs = pickle.load(cache)
            print('Using cached traces')
    except:
        print('No existing trace IDs found in cache')
        pass
    row = {}
    row["traceID"] = None #["traceID", "duration-ms", "startTime", "endTime", "rpcErrors", "operation"]
    row["duration-ms"] = None
    row["startTime"] = None
    row["endTime"] = None
    row["rpcErrors"] = None
    row["operation"] = None
    row["processes"] = None
    with openf(out, compression) as o:
        w = csv.writer(o)
        #w.writerow(row.keys())
        traces = ijson.items(f, 'data.item')
        i = 0
        tot = 0
        ret = []
        for t in traces:
            tot += 1
            if t['traceID'] not in traceIDs:
                traceIDs.add(t['traceID'])
                i += 1
                if i%100==0:
                    # update progress bar
                    print(f'\r==> {i} written traces to {out}', end='', flush=True)
                spans = t['spans']
                endTrace = max([s['startTime'] + s['duration'] for s in spans])
                startTrace = min([s['startTime'] for s in spans])
                traceDuration = endTrace -startTrace
                hasErrors = False
                for si in range(len(spans)):
                    if si == 0:
                        operationName = spans[si]['operationName']
                    for tag in spans[si]['tags']:
                        if tag['key'] == 'error':
                            hasErrors = tag['value']
                            break
                processes = []
                for p in t['processes']:
                    processes.append(t['processes'][p]['serviceName'])
                row["traceID"] = t["traceID"]
                row["duration-ms"] = traceDuration/1000 
                row["startTime"] = startTrace
                row["endTime"] = endTrace
                row["rpcErrors"] = hasErrors
                row["operation"] = operationName
                row['processes'] = ';'.join(set(processes)) # processes involved in this trace
                w.writerow(row.values())
                if debug:
                    ret.append(copy.copy(row))
    print(f'\r==> {i}/{tot} written traces to {out}', end='', flush=True)
    with open(os.path.dirname(out) + '/.traceIDs.pickle', 'wb') as cache:
        print(f'\nRewriting cache of size {len(traceIDs)} to {os.path.dirname(out) + "/.traceIDs.pickle"}')
        pickle.dump(traceIDs, cache)
    with open(os.path.dirname(out) + '/header.csv', 'w') as ff:
        w = csv.writer(ff)
        w.writerow(row.keys())
    if debug:
       return ret


def generate_trace_json(trace: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a JSON representation of a single trace.
    
    :param trace: A dictionary containing trace data
    :return: A dictionary representing the trace in the desired JSON format
    """
    def create_span_dict(span: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "spanID": span["spanID"],
            "operation": span["operationName"],
            "service": trace["processes"][span["processID"]]["serviceName"],
            "startTime": span["startTime"],
            "endTime": span["startTime"] + span["duration"],
            "duration": span["duration"],
            "error": any(tag["key"] == "error" and tag["value"] for tag in span["tags"]),
            "children": []
        }

    spans = {span["spanID"]: create_span_dict(span) for span in trace["spans"]}

    for span in trace["spans"]:
        if "references" in span:
            for ref in span["references"]:
                if ref["refType"] == "CHILD_OF":
                    parent_span = spans[ref["spanID"]]
                    parent_span["children"].append(spans[span["spanID"]])

    root_span = min(spans.values(), key=lambda x: x['startTime'])

    def sort_children_recursively(span):
        if span['children']:
            span['children'].sort(key=lambda x: x['startTime'])
            for child in span['children']:
                sort_children_recursively(child)
        return span

    final_root_span = sort_children_recursively(root_span)

    return {
        "traceID": trace["traceID"],
        "rootSpan": final_root_span
    }

def process_all_traces(input_file: str, output_dir: str):
    """
    Process all traces from the input file and generate individual JSON files.
    
    :param input_file: Path to the input file containing all traces
    :param output_dir: Directory to store individual trace JSON files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    with open(input_file, 'rb') as f:
        traces = ijson.items(f, 'data.item')
        
        for i, trace in enumerate(traces):
            trace_json = generate_trace_json(trace)
            output_file = os.path.join(output_dir, f"{trace['traceID']}.json")
            
            with open(output_file, 'w') as out_f:
                json.dump(trace_json, out_f, indent=2)
            
            if (i + 1) % 100 == 0:
                print(f"\rProcessed {i + 1} traces", end="", flush=True)

    print(f"\nCompleted processing all traces. Output files are in {output_dir}")


