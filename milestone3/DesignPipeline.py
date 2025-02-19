import cv2
import uuid
import json
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# ===============================================================
# 1. Parse Pub/Sub Message to Get an Image
# ===============================================================
class ParseImage(beam.DoFn):
    def process(self, element):
        """
        Expects a Pub/Sub message containing raw image bytes.
        Decodes the image and assigns a unique message ID.
        """
        # Generate a unique message ID
        msg_id = str(uuid.uuid4())

        # element is of type bytes
        image_array = np.frombuffer(element, dtype=np.uint8)
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        if img is None:
            return  # skip if image decoding fails
        yield (msg_id, img)


# ===============================================================
# 2. YOLO Inference DoFn: Detect Pedestrians Only
# ===============================================================
class YOLOInference(beam.DoFn):
    def setup(self):
        # Import YOLO from the ultralytics package.
        # Ensure that your Docker image has installed ultralytics (or your chosen YOLO package)
        from ultralytics import YOLO
        # Load your pretrained YOLO model (adjust model path as needed)
        self.model = YOLO("yolo11n.pt")

    def process(self, element):
        """
        For each image, run YOLO and filter for class 'person' (assumed to be class_id == 0).
        Yields a tuple: (msg_id, {'image': img, 'person_boxes': [...]})
        Each bounding box is represented as (x1, y1, x2, y2, confidence).
        """
        msg_id, img = element

        # Run inference with YOLO. (If your API differs, adjust accordingly.)
        results = self.model(img, stream=True)
        person_boxes = []
        for result in results:
            boxes = result.boxes
            for box in boxes:
                class_id = int(box.cls)
                # Filter for 'person' (class 0)
                if class_id == 0:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    conf = box.conf[0].item()
                    person_boxes.append((x1, y1, x2, y2, conf))

        # Only emit if at least one person was detected.
        if person_boxes:
            yield (msg_id, {"image": img, "person_boxes": person_boxes})


# ===============================================================
# 3. MiDaS Inference DoFn: Compute Depth Map
# ===============================================================
class MiDaSInference(beam.DoFn):
    def __init__(self, model_type="dpt_levit_224"):
        self.model_type = model_type

    def setup(self):
        import torch
        # Set device (GPU if available, else CPU)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        # Load MiDaS model and its transforms via Torch Hub
        self.midas = torch.hub.load("intel-isl/MiDaS", self.model_type)
        self.midas_transforms = torch.hub.load("intel-isl/MiDaS", "transforms")
        if self.model_type == "MiDaS_small":
            self.transform = self.midas_transforms.small_transform
        else:
            self.transform = self.midas_transforms.default_transform
        self.midas.to(self.device)
        self.midas.eval()

    def process(self, element):
        """
        Receives a tuple: (msg_id, image) and produces (msg_id, depth_map).
        The depth map is resized to the original image dimensions.
        """
        import torch
        msg_id, img = element
        # Convert image to RGB as required by MiDaS
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        input_batch = self.transform(img_rgb).to(self.device)
        with torch.no_grad():
            prediction = self.midas(input_batch)
            # Resize the depth map to match the original image size
            prediction = torch.nn.functional.interpolate(
                prediction.unsqueeze(1),
                size=img.shape[:2],
                mode="bicubic",
                align_corners=False,
            ).squeeze()
        depth_map = prediction.cpu().numpy()
        yield (msg_id, depth_map)


# ===============================================================
# 4. Combine YOLO and MiDaS Results to Compute Distance
# ===============================================================
class ComputeDistance(beam.DoFn):
    def process(self, element):
        """
        Element is a tuple: (msg_id, {'yolo': [yolo_result], 'midas': [depth_map]}).
        For each person detection, extract the depth region using the YOLO bounding box,
        and compute (for example) the average depth.
        Yields a JSON string with the message ID, bounding boxes, and average depths.
        """
        msg_id, grouped = element
        yolo_results = grouped.get("yolo", [])
        midas_results = grouped.get("midas", [])

        if not yolo_results or not midas_results:
            return

        # We assume one result per msg_id in each branch.
        yolo_output = yolo_results[0]  # Contains {"image": img, "person_boxes": [...]}
        depth_map = midas_results[0]    # The depth map array

        person_boxes = yolo_output.get("person_boxes", [])
        detections = []
        for (x1, y1, x2, y2, conf) in person_boxes:
            # Extract depth values for the given bounding box region
            depth_region = depth_map[y1:y2, x1:x2]
            if depth_region.size == 0:
                continue
            avg_depth = float(np.mean(depth_region))
            detections.append({
                "bbox": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
                "confidence": conf,
                "avg_depth": avg_depth
            })

        result = {
            "msg_id": msg_id,
            "detections": detections
        }
        yield json.dumps(result)


# ===============================================================
# 5. Build the Dataflow Pipeline
# ===============================================================
def run(argv=None):
    # Replace these with your GCP project and Pub/Sub topic names.
    PROJECT_ID = "coudcompproj"
    INPUT_TOPIC = "projects/{}/topics/imageInMS3".format(PROJECT_ID)
    OUTPUT_TOPIC = "projects/{}/topics/dataOutMS3".format(PROJECT_ID)

    # Configure Beam pipeline options.
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # 5a. Read messages from the input Pub/Sub topic.
        messages = (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
            | "ParseImage" >> beam.ParDo(ParseImage())
        )

        # 5b. Run YOLO and MiDaS in parallel.
        yolo_results = messages | "YOLO Inference" >> beam.ParDo(YOLOInference())
        midas_results = messages | "MiDaS Inference" >> beam.ParDo(MiDaSInference())

        # 5c. Key results by the unique message ID.
        yolo_kv = yolo_results | "Key YOLO" >> beam.Map(lambda x: (x[0], x[1]))
        midas_kv = midas_results | "Key MiDaS" >> beam.Map(lambda x: (x[0], x[1]))

        # Apply a fixed window of 60 seconds to each PCollection.
        windowed_yolo = yolo_kv | "Window YOLO" >> beam.WindowInto(FixedWindows(60))
        windowed_midas = midas_kv | "Window MiDaS" >> beam.WindowInto(FixedWindows(60))

        # Now join the two windowed streams.
        joined = (
            {"yolo": windowed_yolo, "midas": windowed_midas}
            | "CoGroupByMsgID" >> beam.CoGroupByKey()
        )

        # 5e. Compute the average depth for each detected pedestrian.
        distances = joined | "Compute Distance" >> beam.ParDo(ComputeDistance())

        # 5f. Publish the results to the output Pub/Sub topic.
        distances | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=OUTPUT_TOPIC)


if __name__ == "__main__":
    run()
