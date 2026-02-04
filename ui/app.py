from flask import Flask, render_template
from pymongo import MongoClient
import config
import query_helpers

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient(config.MONGO_URI)
db = client[config.DATABASE_NAME]
results_collection = db[config.RESULTS_COLLECTION]
claims_collection = db[config.CLAIMS_COLLECTION]

@app.route('/')
def dashboard():
    result = results_collection.find_one(sort=[("timestamp", -1)])
    
    if not result:
        return "No data found. Please run analysis first!"

    total_issue_types = 0
    module_issue_types = {}
    
    for module_name in ['claims', 'charges', 'diagnosis', 'cpt', 'adjustment']:
        if module_name in result:
            module_data = result[module_name]
            issues = module_data.get('issues') or module_data.get('Issues', {})
            
            issue_count = len(issues)
            total_issue_types += issue_count
            module_issue_types[module_name] = issue_count
    
    return render_template('dashboard.html', 
                          data=result,
                          total_issue_types=total_issue_types,
                          module_issue_types=module_issue_types)

@app.route('/module/<name>')
def module(name):
    result = results_collection.find_one(sort=[("timestamp", -1)])
    
    if not result or name not in result:
        return f"Module '{name}' not found"
    
    module_data = result[name]
    issues = module_data.get('issues') or module_data.get('Issues', {})
    
    return render_template('module.html', 
                          module_name=name,
                          issues=issues,
                          total_claims=result['overview']['total_claims'])

@app.route('/issue/<module_name>/<issue_name>')
def issue_detail(module_name, issue_name):
    result = results_collection.find_one(sort=[("timestamp", -1)])
    
    # Get issue data
    module_data = result.get(module_name, {})
    issues = module_data.get('issues') or module_data.get('Issues', {})
    issue_data = issues.get(issue_name, {})
    
    examples = fetch_examples(issue_name)
    
    return render_template('issue_detail.html',
                          module_name=module_name,
                          issue_name=issue_name,
                          issue_data=issue_data,
                          examples=examples)

def fetch_examples(issue_name, limit=10):
    pipeline = query_helpers.get_example_pipeline(issue_name, limit)
    
    if pipeline:
        try:
            examples = list(claims_collection.aggregate(pipeline))
            return examples
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    return []

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080, use_reloader=False)