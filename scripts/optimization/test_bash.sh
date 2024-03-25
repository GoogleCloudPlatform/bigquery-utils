for i in "$@"; do
  case $i in
    --input_table_name=*)
      input_table_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_id_col_name=*)
      input_table_id_col_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_query_text_col_name=*)
      input_table_query_text_col_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_slots_col_name=*)
      input_table_slots_col_name="${i#*=}"
      shift # past argument=value
      ;;
    -*|--*)
      echo "Unknown option $i"
      exit 1
      ;;
    *)
      ;;
  esac
done

echo $input_table_name
echo $input_table_id_col_name
echo $input_table_query_text_col_name
echo $input_table_slots_col_name