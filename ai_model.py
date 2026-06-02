import pickle

# =========================
# LOAD MODEL ONCE
# =========================
bundle = pickle.load(open("model_bundle.pkl", "rb"))

job_pipeline = bundle["job_pipeline"]
le = bundle["label_encoder"]
salary_model = bundle["salary_model"]


# =========================
# MAIN FUNCTION
# =========================
def predict(skills):
    """
    skills: list of strings
    return: job + salary range
    """

    text = " ".join(skills)

    # =========================
    # PREDICT JOB
    # =========================
    job = job_pipeline.predict([text])[0]

    # =========================
    # PREDICT SALARY
    # =========================
    job_encoded = le.transform([job])[0]
    base_salary = salary_model.predict([[job_encoded]])[0]

    # LIMIT RANGE
    base_salary = max(300, min(3000, base_salary))

    salary_min = max(300, base_salary - 300)
    salary_max = min(3000, base_salary + 500)

    # =========================
    # RETURN RESULT
    # =========================
    return {
        "job": job,
        "salary_min": round(salary_min, 2),
        "salary_max": round(salary_max, 2)
    }