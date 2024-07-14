import requests
import json
import os
import ijson
from typing import Dict, List, Any


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

def generate_trace_json(trace: Dict[str, Any]) -> Dict[str, Any]:
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


