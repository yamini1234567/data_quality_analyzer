from flask import Flask, render_template, request
from pymongo import MongoClient
import config
import query_helpers

app = Flask(__name__)

client = MongoClient(config.MONGO_URI)
db = client[config.DATABASE_NAME]
results_collection = db[config.RESULTS_COLLECTION]
claims_collection = db[config.CLAIMS_COLLECTION]

def get_available_payers():
    pipeline = [
        {"$match": {"payer": {"$ne": None}, "payer": {"$exists": True}}},
        {"$group": {"_id": "$payer"}},
        {"$sort": {"_id": 1}}
    ]
    payers = list(results_collection.aggregate(pipeline))
    return [p["_id"] for p in payers if p["_id"]]

def get_latest_timestamp():
    result = results_collection.find_one(
        sort=[("timestamp", -1)],
        projection={"timestamp": 1}
    )
    return result["timestamp"] if result else None

def count_non_zero_issues(issues):
    if not isinstance(issues, dict):
        return 0
    
    count = 0
    for issue_name, issue_data in issues.items():
        if isinstance(issue_data, dict):
            if issue_data.get('count', 0) > 0:
                count += 1
    return count

@app.route('/')
def dashboard():
    selected_payer = request.args.get('payer', None)
    available_payers = get_available_payers()
    
    if not selected_payer and available_payers:
        selected_payer = available_payers[0]
    
    if not selected_payer:
        return "No payer data found. Please run analysis first!"
    
    latest_timestamp = get_latest_timestamp()
    
    overview_doc = results_collection.find_one({
        "analysis_type": "overview",
        "timestamp": latest_timestamp
    })
    
    if not overview_doc:
        return "No overview data found. Please run analysis first!"
    
    query = {
        "payer": selected_payer,
        "timestamp": latest_timestamp
    }
    
    module_docs = {}
    for analysis_type in ['claims', 'charges', 'diagnosis', 'cpt', 'adjustment']:
        doc = results_collection.find_one({
            **query,
            "analysis_type": analysis_type
        })
        if doc:
            module_docs[analysis_type] = doc
    
    total_issue_types = 0
    module_issue_types = {}
    
    for module_name in ['claims', 'charges', 'diagnosis', 'cpt', 'adjustment']:
        if module_name in module_docs:
            quality_check = module_docs[module_name].get('quality_check', {})
            issues = quality_check.get('issues') or quality_check.get('Issues', {})
            issue_count = count_non_zero_issues(issues)
            total_issue_types += issue_count
            module_issue_types[module_name] = issue_count
        else:
            module_issue_types[module_name] = 0
    
    payer_doc = results_collection.find_one({
        "analysis_type": "payer",
        "timestamp": latest_timestamp
    })
    
    return render_template('dashboard.html',
                          overview=overview_doc.get('quality_check', {}),
                          timestamp=latest_timestamp,
                          selected_payer=selected_payer,
                          available_payers=available_payers,
                          total_issue_types=total_issue_types,
                          module_issue_types=module_issue_types,
                          module_docs=module_docs,
                          payer_data=payer_doc.get('quality_check', {}) if payer_doc else None)

@app.route('/module/<name>')
def module(name):
    selected_payer = request.args.get('payer', None)
    
    if not selected_payer:
        available_payers = get_available_payers()
        if available_payers:
            selected_payer = available_payers[0]
        else:
            return "No payer data found!"
    
    latest_timestamp = get_latest_timestamp()
    
    doc = results_collection.find_one({
        "payer": selected_payer,
        "analysis_type": name,
        "timestamp": latest_timestamp
    })
    
    if not doc:
        return f"Module '{name}' not found for payer '{selected_payer}'"
    
    quality_check = doc.get('quality_check', {})
    issues = quality_check.get('issues') or quality_check.get('Issues', {})
    
    total_claims = 0
    if 'total_claims' in quality_check:
        total_claims = quality_check.get('total_claims', 0)
    else:
        total_claims = claims_collection.count_documents({"payerMCO": selected_payer})
    
    non_zero_issue_count = count_non_zero_issues(issues)
    
    statistics = None
    ranges = None
    high_value = None
    low_value = None
    
    if name == 'charges':
        statistics = quality_check.get('statistics', {})
        ranges = quality_check.get('ranges', [])
        high_value = quality_check.get('high_value', {})
        low_value = quality_check.get('low_value', {})
    
    available_payers = get_available_payers()
    
    return render_template('module.html',
                          module_name=name,
                          issues=issues,
                          total_claims=total_claims,
                          selected_payer=selected_payer,
                          available_payers=available_payers,
                          non_zero_issue_count=non_zero_issue_count,
                          statistics=statistics,
                          ranges=ranges,
                          high_value=high_value,
                          low_value=low_value,
                          quality_check=quality_check)
    
    
@app.route('/issue/<module_name>/<issue_name>')
def issue_detail(module_name, issue_name):
    selected_payer = request.args.get('payer', None)
    
    if not selected_payer:
        available_payers = get_available_payers()
        if available_payers:
            selected_payer = available_payers[0]
        else:
            return "No payer data found!"
    
    latest_timestamp = get_latest_timestamp()
    
    doc = results_collection.find_one({
        "payer": selected_payer,
        "analysis_type": module_name,
        "timestamp": latest_timestamp
    })
    
    if not doc:
        return f"Data not found for payer '{selected_payer}'"
    
    quality_check = doc.get('quality_check', {})
    issues = quality_check.get('issues') or quality_check.get('Issues', {})
    issue_data = issues.get(issue_name, {})
    
    examples = fetch_examples(issue_name, selected_payer)
    available_payers = get_available_payers()
    
    return render_template('issue_detail.html',
                          module_name=module_name,
                          issue_name=issue_name,
                          issue_data=issue_data,
                          examples=examples,
                          selected_payer=selected_payer,
                          available_payers=available_payers)

def fetch_examples(issue_name, payer, limit=10):
    pipeline = query_helpers.get_example_pipeline(issue_name, limit)
    
    if pipeline:
        try:
            if pipeline and len(pipeline) > 0:
                if "$match" in pipeline[0]:
                    pipeline[0]["$match"]["payerMCO"] = payer
                else:
                    pipeline.insert(0, {"$match": {"payerMCO": payer}})
            
            examples = list(claims_collection.aggregate(pipeline))
            return examples
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    return []

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080, use_reloader=False)