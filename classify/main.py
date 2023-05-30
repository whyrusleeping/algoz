from bottle import route, request, run
from PIL import Image
from io import BytesIO
import torch
from torchvision.transforms import Compose, Resize, CenterCrop, ToTensor
import clip
from clip import load

# Load the CLIP model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = load("ViT-B/32", device=device)


# Image transformation pipeline
transform = Compose([
    Resize(256),
    CenterCrop(224),
    ToTensor()
])

@route('/classify_image/', method='POST')
def classify_image():
    # Get categories from query parameters
    categories = request.query.getall('category')

    upload = request.files.get('upload')
    raw = upload.file.read()  # this is dangerous for big files
    image = Image.open(BytesIO(raw)).convert('RGB')
    
    # Preprocess the image
    image = transform(image)
    image = image.unsqueeze(0).to(device)
    
    print("categories: ", categories)

    # Prepare the text inputs
    text = torch.cat([clip.tokenize(f"a photo of a {c}") for c in categories]).to(device)
    
    # Compute the features and compare the image to the text inputs
    with torch.no_grad():
        image_features = model.encode_image(image)
        text_features = model.encode_text(text)
        
    # Compute the raw similarity score
    similarity = (100.0 * image_features @ text_features.T)
    similarity_softmax = similarity.softmax(dim=-1)
    
    # Define a threshold
    threshold = 10.0

    # Get the highest scoring category
    max_raw_score = torch.max(similarity)
    if max_raw_score < threshold:
        return {
            "file_size": len(raw), 
            "category": "none", 
            "similarity_score": 0,
            "values": [0.0 for _ in categories]
        }
    else:
        category_index = similarity_softmax[0].argmax().item()
        category = categories[category_index]
        similarity_score = similarity_softmax[0, category_index].item()
        values = similarity[0].tolist()
        return {
            "file_size": len(raw), 
            "category": category, 
            "similarity_score": similarity_score,
            "values": values
        }


run(host='0.0.0.0', port=8181)

