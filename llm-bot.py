from transformers import AutoTokenizer, AutoModelForCausalLM
from data import travel_df, cust_df

tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen2.5-1.5B-Instruct")
model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen2.5-1.5B-Instruct")

input_text = f"{cust_df}\n{travel_df}\nThere are two tables with travels and customers data, what can you tell based on these tables?"
# Generate response
input_ids = tokenizer.encode(input_text, return_tensors="pt")
output = model.generate(input_ids, max_length=5000)

# Decode and print the generated response
response = tokenizer.decode(output[0], skip_special_tokens=True)
print(response)

